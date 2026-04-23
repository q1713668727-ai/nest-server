import { Injectable } from '@nestjs/common';
import { mkdir, writeFile } from 'fs/promises';
import ffmpegInstaller from '@ffmpeg-installer/ffmpeg';
import ffmpeg from 'fluent-ffmpeg';
import { join } from 'path';
import { getSafeAccountPath, normalizeVideoItem, timestampToTime } from '../common/content.utils';
import { DbService } from '../db/db.service';

@Injectable()
export class VideoService {
  private readonly videoChunksMap: Record<string, { hash: number; chunk: Buffer | string }[]> = {};
  private ensureVideoBriefTask: Promise<void> | null = null;
  private ensureVideoHiddenTask: Promise<void> | null = null;

  constructor(private readonly db: DbService) {}

  private async ensureVideoBriefColumn() {
    if (this.ensureVideoBriefTask) return this.ensureVideoBriefTask;
    this.ensureVideoBriefTask = (async () => {
      const columns = await this.db.query<any>('SHOW COLUMNS FROM `video`;');
      const names = new Set(columns.map((item) => item.Field));
      if (!names.has('brief')) {
        await this.db.query('ALTER TABLE `video` ADD COLUMN `brief` TEXT NULL;');
      }
    })().catch((err) => {
      this.ensureVideoBriefTask = null;
      throw err;
    });
    return this.ensureVideoBriefTask;
  }

  private async ensureVideoHiddenColumn() {
    if (this.ensureVideoHiddenTask) return this.ensureVideoHiddenTask;
    this.ensureVideoHiddenTask = (async () => {
      const columns = await this.db.query<any>('SHOW COLUMNS FROM `video`;');
      const names = new Set(columns.map((item) => item.Field));
      if (!names.has('hidden')) {
        await this.db.query('ALTER TABLE `video` ADD COLUMN `hidden` TINYINT(1) NOT NULL DEFAULT 0;');
      }
    })().catch((err) => {
      this.ensureVideoHiddenTask = null;
      throw err;
    });
    return this.ensureVideoHiddenTask;
  }

  private async generateVideoCover(videoPath: string, coverPath: string) {
    if (ffmpegInstaller?.path) {
      ffmpeg.setFfmpegPath(ffmpegInstaller.path);
    }

    await new Promise<void>((resolve, reject) => {
      ffmpeg(videoPath)
        .inputOptions(['-ss 0.1'])
        .outputOptions(['-frames:v 1', '-q:v 2'])
        .output(coverPath)
        .on('end', () => resolve())
        .on('error', (error) => reject(error))
        .run();
    });
  }

  async list(body: any = {}) {
    try {
      await this.ensureVideoBriefColumn();
      await this.ensureVideoHiddenColumn();
      const targetId = Number(body.targetId ?? body.id ?? 0);
      const account = String(body.account || '').trim();
      const targetRows = Number.isFinite(targetId) && targetId > 0
        ? await this.db.query<any>(
            'SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM `video` v LEFT JOIN `login` l ON l.account = v.account WHERE v.id = ? AND (COALESCE(v.`hidden`, 0) = 0 OR v.`account` = ?) LIMIT 1;',
            [targetId, account],
          )
        : [];
      const rows = await this.db.query<any>(
        'SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM `video` v LEFT JOIN `login` l ON l.account = v.account WHERE COALESCE(v.`hidden`, 0) = 0 ORDER BY RAND() LIMIT 30;',
      );
      const mergedRows = [...targetRows, ...rows.filter((item) => !targetRows.some((target) => String(target.id) === String(item.id)))];
      const result = mergedRows.map((item) => ({
        ...normalizeVideoItem(item),
        name: item.name || item.authorName || item.account,
        avatar: item.authorAvatar || '',
      }));
      return { status: 200, message: '', result };
    } catch (err: any) {
      return { status: 500, message: 'Fetch failed', error: err.toString() };
    }
  }

  addvideo(body: any, file?: any) {
    const { account } = body;
    const hash = body.hash !== undefined ? body.hash : body.task;
    const chunkBuffer = file?.buffer;
    const chunkString = typeof body.chunk === 'string' ? body.chunk : null;
    if (!account || hash === undefined || (!chunkBuffer && !chunkString)) {
      return { status: 400, message: 'Missing chunk payload.' };
    }
    if (!this.videoChunksMap[account]) this.videoChunksMap[account] = [];
    this.videoChunksMap[account].push({ hash: parseInt(hash, 10), chunk: chunkBuffer || chunkString });
    return { status: 200, message: 'Chunk stored.' };
  }

  async addvideoEnd(body: any) {
    const { account, type, title, name } = body;
    const safeAccount = getSafeAccountPath(account);
    if (!safeAccount) return { status: 400, message: 'Invalid account.' };
    const time = Date.now();
    const date = timestampToTime(time);
    const userChunks = this.videoChunksMap[account];
    if (!userChunks || !userChunks.length) return { status: 400, message: 'No video chunks found.' };
    try {
      await this.ensureVideoBriefColumn();
      userChunks.sort((a, b) => a.hash - b.hash);
      let totalBuffer: Buffer;
      if (Buffer.isBuffer(userChunks[0].chunk)) {
        totalBuffer = Buffer.concat(userChunks.map((item) => item.chunk as Buffer));
      } else {
        let mergedBase64 = userChunks.map((item) => String(item.chunk || '')).join('');
        if (mergedBase64.includes(',')) mergedBase64 = mergedBase64.split(',')[1];
        totalBuffer = Buffer.from(mergedBase64, 'base64');
      }
      const dirPath = `public/video/${safeAccount}`;
      const coverDirPath = `public/video-cover/${safeAccount}`;
      await mkdir(dirPath, { recursive: true });
      await mkdir(coverDirPath, { recursive: true });
      const videoFileName = `${time}.${type}`;
      const videoFilePath = `${dirPath}/${videoFileName}`;
      const coverFileName = `${time}.jpg`;
      const coverFilePath = `${coverDirPath}/${coverFileName}`;
      await writeFile(videoFilePath, totalBuffer);

      try {
        await this.generateVideoCover(join(process.cwd(), videoFilePath), join(process.cwd(), coverFilePath));
      } catch (coverError: any) {
        return { status: 500, message: 'Video cover generation failed.', error: coverError?.toString?.() || String(coverError) };
      }

      const imagePath = `video-cover/${safeAccount}/${coverFileName}`;
      const result: any = await this.db.query(
        'INSERT INTO `video`(`base`,`image`,`account`,`title`,`brief`,`date`,`likes`,`name`,`url`,`comment`) VALUES(?,?,?,?,?,?,?,?,?,?)',
        [`video/${type}`, imagePath, account, title, String(body.brief || '').trim(), date, '0', name, videoFileName, ''],
      );
      const affected = result[0]?.affectedRows ?? result.affectedRows ?? 0;
      if (affected === 1) {
        delete this.videoChunksMap[account];
        return { status: 200, message: 'Video uploaded successfully.' };
      }
      return { status: 500, message: 'Database insert failed.' };
    } catch (error: any) {
      return { status: 500, message: 'Video merge failed', error: error.toString() };
    }
  }
}
