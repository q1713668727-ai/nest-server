import { Injectable } from '@nestjs/common';
import { mkdir, writeFile } from 'fs/promises';
import { getSafeAccountPath, timestampToTime } from '../common/content.utils';
import { DbService } from '../db/db.service';

@Injectable()
export class FileService {
  private fileBox: any[] = Array.from({ length: 500 }, () => null);
  private noteArr: any[] = [];
  private videoArr: any[] = [];

  constructor(private readonly db: DbService) {}

  uploadFile(body: any) {
    if (!body?.data || body.data.hash === undefined) return { status: 400, message: 'Missing file chunk.' };
    this.fileBox[body.data.hash] = body;
    return { status: 200, message: 'Upload success.' };
  }

  async uploadEnd(body: any) {
    let file = '';
    const err: number[] = [];
    for (let i = 0; i < this.fileBox.length; i += 1) {
      const item = this.fileBox[i];
      if (item == null) break;
      if (item.UserToUser === body.UserToUser && item.target === body.target && item.account === body.account && item.url === body.url) {
        if (item.data.hash === i) file += item.data.chunk;
      } else err.push(item.data.hash);
    }
    if (err.length) {
      this.fileBox = [];
      return { status: 404, message: 'File is incomplete.' };
    }
    this.fileBox = [];
    const base64 = file.split(';')[1]?.split(',')[1];
    const safeUserToUser = getSafeAccountPath(body.UserToUser);
    const path = `public/user-message/${safeUserToUser}/${body.message.text.url}.${body.message.text.type}`;
    await mkdir(`public/user-message/${safeUserToUser}`, { recursive: true });
    await writeFile(path, Buffer.from(base64, 'base64'));
    return { status: 200, message: 'Upload finished.' };
  }

  addnote(body: any, file?: any) {
    if (file?.buffer) {
      const account = String(body.account || '').trim();
      const uploadId = String(body.uploadId || '').trim();
      const key = String(body.key || '').trim();
      const hash = Number(body.hash ?? body.index ?? 0);
      const type = String(body.type || file.originalname?.split('.').pop() || 'jpg').replace(/[^a-zA-Z0-9]/g, '').toLowerCase();
      const base = String(body.base || file.mimetype || `image/${type}`);
      if (!account || !uploadId || !key || !Number.isFinite(hash)) return { status: 400, message: 'Missing note file params.' };
      this.noteArr.push({ uploadId, account, key, hash, base, type, buffer: file.buffer });
      return { status: 200, message: 'Image uploaded.' };
    }

    this.noteArr.push({ list: body.list, account: body.account, base: body.base, type: body.type, uploadId: body.uploadId || '' });
    return { status: 200, message: 'Upload success.' };
  }

  async addnoteEnd(body: any) {
    const time = Date.now();
    const date = timestampToTime(time);
    const uploadId = String(body.uploadId || '').trim();
    const data = this.noteArr.filter((item) => item && item.account === body.account && (!uploadId || item.uploadId === uploadId || !item.uploadId));
    const arr: any[][] = [];
    const obj: any[] = [];
    body.key.forEach((item: any, idx: number) => {
      arr[idx] = [];
      for (let i = 0; i < data.length; i += 1) {
        const dataKey = data[i].key ?? data[i].list?.id;
        if (dataKey === item) {
          if (data[i].base === 'image/jpeg') data[i].base = 'image/jpg';
          obj[idx] = { type: data[i].type, base: data[i].base };
          arr[idx].push(data[i].buffer ? { hash: data[i].hash, buffer: data[i].buffer } : data[i].list);
        }
      }
    });
    for (let idx = 0; idx < arr.length; idx += 1) {
      const item = arr[idx];
      if (!item.length) return { status: 400, message: 'Image file is missing.' };
      const imgArr: string[] = [];
      const bufferArr: Buffer[] = [];
      item.forEach((li) => {
        if (li.buffer) bufferArr[li.hash] = li.buffer;
        else imgArr[li.hash] = li.chunk;
      });
      const safeAccount = getSafeAccountPath(body.account);
      const path = `public/note-image/${safeAccount}/${time}-${body.key[idx]}.${obj[idx].type}`;
      await mkdir(`public/note-image/${safeAccount}`, { recursive: true });
      if (bufferArr.length) {
        await writeFile(path, Buffer.concat(bufferArr.filter(Boolean)));
      } else {
        let base64 = '';
        for (let x = 0; x < imgArr.length; x += 1) base64 += imgArr[x] || '';
        base64 = base64.split(';')[1]?.split(',')[1];
        await writeFile(path, Buffer.from(base64, 'base64'));
      }
    }
    let image = '';
    let base = '';
    obj.forEach((item, idx) => {
      image += `${time}-${body.key[idx]}.${item.type}${obj.length === 1 ? '' : '/'}`;
      base += `${item.base}${obj.length === 1 ? '' : ';'}`;
    });
    const result: any = await this.db.query(
      'INSERT INTO `note`(`base`,`image`,`account`,`title`,`brief`,`date`,`likes`,`name`,`url`) VALUES(?,?,?,?,?,?,?,?,?)',
      [base, image, body.account, body.title, body.brief, date, '0', body.name, body.url],
    );
    this.noteArr = this.noteArr.filter((item) => item && (item.account !== body.account || (uploadId && item.uploadId !== uploadId)));
    return (result[0]?.affectedRows ?? result.affectedRows ?? 0) === 1
      ? { status: 200, message: 'Note saved.' }
      : { status: 404, message: 'Note insert failed.' };
  }

  addvideo(body: any) {
    this.videoArr.push({ ...body.list, account: body.account });
    return { status: 200, message: 'Upload success.' };
  }

  async addvideoEnd(body: any) {
    const time = Date.now();
    const date = timestampToTime(time);
    const data = this.videoArr.filter((item) => item && item.account === body.account);
    data.forEach((item) => {
      this.videoArr[item.hash] = item.chunk;
    });
    let base64 = '';
    for (let x = 0; x < this.videoArr.length; x += 1) base64 += this.videoArr[x] || '';
    base64 = base64.split(';')[1]?.split(',')[1];
    const safeAccount = getSafeAccountPath(body.account);
    const path = `public/video/${safeAccount}/${time}.${body.type}`;
    await mkdir(`public/video/${safeAccount}`, { recursive: true });
    await writeFile(path, Buffer.from(base64, 'base64'));
    const result: any = await this.db.query(
      'INSERT INTO `video`(`base`,`image`,`account`,`title`,`date`,`likes`,`name`,`url`,`comment`) VALUES(?,?,?,?,?,?,?,?,?)',
      [`video/${body.type}`, body.url, body.account, body.title, date, '0', body.name, `${time}.${body.type}`, ''],
    );
    this.videoArr = this.videoArr.filter((item) => item && item.account !== body.account);
    return (result[0]?.affectedRows ?? result.affectedRows ?? 0) === 1
      ? { status: 200, message: 'Video saved.' }
      : { status: 404, message: 'Video insert failed.' };
  }
}
