import { Injectable } from '@nestjs/common';
import { DbService } from '../db/db.service';
import {
  normalizeLikeUsers,
  normalizeVideoItem,
  parseNoteComments,
  toSafeNumber,
} from '../common/content.utils';

@Injectable()
export class IndexService {
  constructor(private readonly db: DbService) {}

  private shuffleList(list: any[]) {
    const arr = [...list];
    for (let i = arr.length - 1; i > 0; i -= 1) {
      const j = Math.floor(Math.random() * (i + 1));
      [arr[i], arr[j]] = [arr[j], arr[i]];
    }
    return arr;
  }

  async index(body: any) {
    try {
      const num = body.init ? 20 : 10;
      const idTokens = String(body.idList || '')
        .split(',')
        .map((item) => String(item).trim())
        .filter(Boolean);
      const excludedNoteIds: number[] = [];
      const excludedVideoIds: number[] = [];
      idTokens.forEach((token) => {
        if (token.startsWith('note-')) {
          const id = Number(token.slice(5));
          if (Number.isFinite(id)) excludedNoteIds.push(id);
          return;
        }
        if (token.startsWith('video-')) {
          const id = Number(token.slice(6));
          if (Number.isFinite(id)) excludedVideoIds.push(id);
          return;
        }
        const legacyId = Number(token);
        if (Number.isFinite(legacyId)) excludedNoteIds.push(legacyId);
      });

      const typeLimit = Math.max(num * 2, 20);
      let noteSql = 'SELECT * FROM `note` ORDER BY RAND() LIMIT ?;';
      let noteParams: any[] = [typeLimit];
      let videoSql =
        'SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM `video` v LEFT JOIN `login` l ON l.account = v.account ORDER BY RAND() LIMIT ?;';
      let videoParams: any[] = [typeLimit];
      if (excludedNoteIds.length > 0) {
        const placeholders = excludedNoteIds.map(() => '?').join(',');
        noteSql = `SELECT * FROM \`note\` WHERE \`id\` NOT IN (${placeholders}) ORDER BY RAND() LIMIT ?;`;
        noteParams = [...excludedNoteIds, typeLimit];
      }
      if (excludedVideoIds.length > 0) {
        const placeholders = excludedVideoIds.map(() => '?').join(',');
        videoSql = `SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM \`video\` v LEFT JOIN \`login\` l ON l.account = v.account WHERE v.\`id\` NOT IN (${placeholders}) ORDER BY RAND() LIMIT ?;`;
        videoParams = [...excludedVideoIds, typeLimit];
      }
      const [noteRows, videoRows] = await Promise.all([this.db.query<any>(noteSql, noteParams), this.db.query<any>(videoSql, videoParams)]);
      const noteList = noteRows.map((item) => ({ ...item, contentType: 'note', feedKey: `note-${item.id}` }));
      const videoList = videoRows.map((item) => normalizeVideoItem(item));
      const result = this.shuffleList([...noteList, ...videoList]).slice(0, num);
      return { status: 200, message: 'Fetch success.', result };
    } catch (err: any) {
      return { status: 500, message: 'Fetch failed', error: err.toString() };
    }
  }

  async noteDetail(body: any) {
    try {
      const noteId = Number(body?.id);
      if (!Number.isFinite(noteId)) return { status: 400, message: 'Invalid note id.' };

      const rows = await this.db.query<any>('SELECT * FROM `note` WHERE `id` = ? LIMIT 1;', [noteId]);
      if (!Array.isArray(rows) || !rows.length) return { status: 404, message: 'Note not found.' };

      const note = rows[0] || {};
      const comments = parseNoteComments(note.comment).map((item: any, index: number) => ({
        id: item?.id ?? index,
        account: String(item?.account || ''),
        name: String(item?.name || item?.account || '用户'),
        text: String(item?.text || ''),
        avatar: String(item?.avatar || ''),
        likeCount: toSafeNumber(item?.likeCount ?? item?.likes ?? item?.likess, 0),
        location: String(item?.location || ''),
        date: String(item?.date || ''),
      }));

      return {
        status: 200,
        message: 'Fetch success.',
        result: {
          ...note,
          comments,
        },
      };
    } catch (err: any) {
      return { status: 500, message: 'Fetch note detail failed', error: err.toString() };
    }
  }

  async clearBadge(body: any) {
    try {
      const target = `${body.account}-${body.targetUser}`;
      const result = await this.db.query<any>('SELECT * FROM `msg` WHERE `UserToUser` = ?;', [target]);
      if (result.length === 0) return { status: 404, message: 'Message record not found.' };
      const messageObj = JSON.parse(result[0].message);
      messageObj.read = 0;
      const updateRes = await this.db.query<any>('UPDATE `msg` SET message = ? WHERE `UserToUser` = ?;', [
        JSON.stringify(messageObj),
        target,
      ]);
      if ((updateRes[0]?.affectedRows ?? 0) === 0) return { status: 404, message: 'Update failed.' };
      return { status: 200, message: 'Badge cleared.' };
    } catch (err: any) {
      return { status: 500, message: 'Clear badge failed', error: err.toString() };
    }
  }

  async deleteUser(body: any) {
    try {
      const t1 = `${body.account}-${body.targetUser}`;
      const t2 = `${body.targetUser}-${body.account}`;
      const res1: any = await this.db.query('DELETE FROM `msg` WHERE `UserToUser` = ?;', [t1]);
      if ((res1[0]?.affectedRows ?? res1.affectedRows) === 0) return { status: 404, message: 'Delete failed.' };
      const res2: any = await this.db.query('DELETE FROM `msg` WHERE `UserToUser` = ?;', [t2]);
      if ((res2[0]?.affectedRows ?? res2.affectedRows) === 0) return { status: 404, message: 'Delete failed.' };
      return { status: 200, message: 'Delete success.' };
    } catch (err: any) {
      return { status: 500, message: 'Delete failed', error: err.toString() };
    }
  }

  async setUserData(body: any) {
    try {
      const fieldMap: Record<string, string> = {
        name: 'name',
        email: 'email',
        sign: 'sign',
        about: 'about',
        avatar: 'avatar',
        background: 'background',
        birthday: 'birthday',
        sex: 'sex',
        occupation: 'occupation',
        school: 'school',
        district: 'district',
        password: 'password',
      };
      const field = fieldMap[String(body.type || '').trim()];
      if (!field) return { status: 400, message: 'Invalid field.' };
      const sql = `UPDATE \`login\` SET \`${field}\` = ? WHERE \`account\` = ?;`;
      const result: any = await this.db.query(sql, [body.data, body.account]);
      const affected = result[0]?.affectedRows ?? result.affectedRows ?? 0;
      return affected === 1 ? { status: 200, message: 'Update success.' } : { status: 404, message: 'Update failed.' };
    } catch (err: any) {
      return { status: 500, message: 'Update failed', error: err.toString() };
    }
  }

  async changePassword(body: any) {
    try {
      const account = String(body.account || '').trim();
      const oldPassword = String(body.oldPassword || '');
      const newPassword = String(body.newPassword || '');

      if (!account || !oldPassword || !newPassword) {
        return { status: 400, message: '请填写原密码和新密码。' };
      }
      if (newPassword.length < 6) {
        return { status: 400, message: '新密码至少需要 6 位。' };
      }
      if (oldPassword === newPassword) {
        return { status: 400, message: '新密码不能和原密码相同。' };
      }

      const rows = await this.db.query<any>('SELECT `password` FROM `login` WHERE `account` = ? LIMIT 1;', [account]);
      if (!rows.length) return { status: 404, message: '账号不存在。' };
      if (String(rows[0].password || '') !== oldPassword) {
        return { status: 401, message: '原密码不正确。' };
      }

      const result: any = await this.db.query('UPDATE `login` SET `password` = ? WHERE `account` = ? LIMIT 1;', [newPassword, account]);
      const affected = result[0]?.affectedRows ?? result.affectedRows ?? 0;
      return affected === 1 ? { status: 200, message: '密码修改成功。' } : { status: 404, message: '密码修改失败。' };
    } catch (err: any) {
      return { status: 500, message: '密码修改失败', error: err.toString() };
    }
  }

  async getAllUser(body: any) {
    try {
      const excludeAccounts = Array.isArray(body.account) ? body.account.map((item) => String(item || '').trim()).filter(Boolean) : [];
      const keyword = String(body.keyword || '').trim();
      const limit = Math.max(Math.min(Number(body.limit) || 10, 50), 1);
      const offset = Math.max(Number(body.offset) || 0, 0);
      const where: string[] = [];
      const params: any[] = [];
      if (excludeAccounts.length) {
        where.push(`\`account\` NOT IN (${excludeAccounts.map(() => '?').join(',')})`);
        params.push(...excludeAccounts);
      }
      if (keyword) {
        where.push('(`name` LIKE ? OR `account` LIKE ?)');
        params.push(`%${keyword}%`, `%${keyword}%`);
      }
      const whereSql = where.length ? `WHERE ${where.join(' AND ')}` : '';
      const countSql = `SELECT COUNT(*) AS total FROM \`login\` ${whereSql};`;
      const listSql = `SELECT * FROM \`login\` ${whereSql} ORDER BY \`account\` DESC LIMIT ? OFFSET ?;`;
      const countResult = await this.db.query<any>(countSql, params);
      const total = Number((countResult[0] && countResult[0].total) || 0);
      const result = await this.db.query<any>(listSql, [...params, limit, offset]);
      const arr = (result || []).map((item) => ({ ...item, url: item.avatar, avatar: undefined, password: undefined }));
      return { status: 200, message: 'Fetch success.', result: arr, total };
    } catch (err: any) {
      return { status: 500, message: 'Fetch users failed', error: err.toString() };
    }
  }

  async getConversation(body: any) {
    try {
      const account = String(body.account || '').trim();
      const target = String(body.target || '').trim();
      if (!account || !target) return { status: 400, message: 'Missing account or target.' };
      const meKey = `${account}-${target}`;
      const targetKey = `${target}-${account}`;
      const result = await this.db.query<any>('SELECT * FROM `msg` WHERE `UserToUser` IN (?, ?);', [meKey, targetKey]);
      let current: any = null;
      let reverse: any = null;
      result.forEach((item) => {
        if (item.UserToUser === meKey) current = item;
        if (item.UserToUser === targetKey) reverse = item;
      });
      if (!current && !reverse) {
        return { status: 200, message: 'First conversation.', firstChat: true, result: null };
      }
      let message: any = {};
      try {
        message = JSON.parse((current || reverse).message || '{}');
      } catch {
        message = {};
      }
      const historyMessage = Array.isArray(message.historyMessage) ? message.historyMessage : [];
      const normalizedHistory = historyMessage.map((item: any) => {
        const nextItem = { ...item, text: item.text ? { ...item.text } : {} };
        if (nextItem.text.type === 'emoji' && nextItem.text.url && !String(nextItem.text.url).startsWith('images/emoji/')) {
          nextItem.text.url = `images/emoji/${nextItem.text.url}`;
        } else if (nextItem.text.type === 'file' && nextItem.text.url) {
          const suffix = String(nextItem.text.url).split('.');
          nextItem.text.suffix = suffix[suffix.length - 1];
        }
        return nextItem;
      });
      return {
        status: 200,
        message: 'Conversation found.',
        firstChat: false,
        result: {
          id: message.id || target,
          title: message.title || target,
          avatar: message.avatar || '',
          url: message.avatar || '',
          read: Number(message.read || 0),
          historyMessage: normalizedHistory,
        },
        syncNeeded: !current || !reverse,
      };
    } catch (err: any) {
      return { status: 500, message: 'Fetch conversation failed', error: err.toString() };
    }
  }

  async add(body: any) {
    try {
      const { me, you } = body;
      if (!me || !you || !me.UserToUser || !you.UserToUser || !me.account || !you.account) {
        return { status: 400, message: 'Invalid add friend payload.' };
      }
      const exists = await this.db.query<any>('SELECT `UserToUser` FROM `msg` WHERE `UserToUser` IN (?, ?);', [me.UserToUser, you.UserToUser]);
      const existingKeys = new Set(exists.map((item) => item.UserToUser));
      const values: string[] = [];
      const params: any[] = [];
      if (!existingKeys.has(me.UserToUser)) {
        values.push('(?,?,?)');
        params.push(me.UserToUser, me.account, me.message);
      }
      if (!existingKeys.has(you.UserToUser)) {
        values.push('(?,?,?)');
        params.push(you.UserToUser, you.account, you.message);
      }
      if (!values.length) return { status: 200, message: 'Conversation ready.' };
      const result: any = await this.db.query(
        `INSERT INTO \`msg\`(\`UserToUser\`,\`account\`,\`message\`) VALUES ${values.join(',')}`,
        params,
      );
      const affected = result[0]?.affectedRows ?? result.affectedRows ?? 0;
      return affected === values.length ? { status: 200, message: 'Conversation ready.' } : { status: 404, message: 'Conversation init failed.' };
    } catch (err: any) {
      return { status: 500, message: 'Add friend failed', error: err.toString() };
    }
  }

  async addComment(body: any) {
    try {
      const noteId = body.id;
      const contentType = String(body.contentType || 'note').trim().toLowerCase();
      const table = contentType === 'video' ? 'video' : 'note';
      if (!noteId) return { status: 400, message: `Missing ${table} id.` };
      const result = await this.db.query<any>(`SELECT \`comment\` FROM \`${table}\` WHERE \`id\` = ?;`, [noteId]);
      if (!result.length) return { status: 404, message: `${table} not found.` };
      const oldComments = parseNoteComments(result[0].comment).map((item: any) => {
        const likeUsers = normalizeLikeUsers(item.likeUsers || item.likeAccounts || item.likeUserInfo)
          .map((user: any) => ({
            account: String(user.account || '').trim(),
            name: user.name || '',
            avatar: user.avatar || '',
          }))
          .filter((user: any) => user.account);
        return {
          ...item,
          id: toSafeNumber(item.id, Date.now()),
          likeUsers,
          likeCount: toSafeNumber(item.likeCount || item.likes, likeUsers.length),
          replies: Array.isArray(item.replies)
            ? item.replies.map((reply: any) => ({
                ...reply,
                id: toSafeNumber(reply.id, Date.now()),
                likeCount: toSafeNumber(reply.likeCount || reply.likes, 0),
              }))
            : [],
        };
      });
      const action = String(body.action || 'add').trim();
      if (action === 'like') {
        const commentId = toSafeNumber(body.commentId);
        const parentId = toSafeNumber(body.parentId);
        const account = String(body.account || '').trim();
        if (!commentId || !account) return { status: 400, message: 'Missing commentId or account.' };
        if (parentId !== 0) return { status: 400, message: 'Only top-level comments support likes.' };
        const targetIndex = oldComments.findIndex((item: any) => toSafeNumber(item.id) === commentId);
        if (targetIndex === -1) return { status: 404, message: 'Comment not found.' };
        const current = oldComments[targetIndex];
        const likeUsers = Array.isArray(current.likeUsers) ? [...current.likeUsers] : [];
        const existsIndex = likeUsers.findIndex((user: any) => String(user.account || '') === account);
        let liked = false;
        if (existsIndex > -1) likeUsers.splice(existsIndex, 1);
        else {
          likeUsers.push({ account, name: body.name || '', avatar: body.avatar || '' });
          liked = true;
        }
        oldComments[targetIndex] = { ...current, likeUsers, likeCount: likeUsers.length };
        const updateLikeRes: any = await this.db.query(`UPDATE \`${table}\` SET \`comment\` = ? WHERE \`id\` = ?;`, [JSON.stringify(oldComments), noteId]);
        const affected = updateLikeRes[0]?.affectedRows ?? updateLikeRes.affectedRows ?? 0;
        if (affected === 1) return { status: 200, message: 'Comment like updated.', result: { commentId, liked, likeCount: likeUsers.length, likeUsers } };
        return { status: 404, message: 'Comment like update failed.' };
      }
      if (action === 'reply') {
        const parentId = toSafeNumber(body.parentId);
        if (!parentId) return { status: 400, message: 'Missing parentId.' };
        const parentIdx = oldComments.findIndex((item: any) => toSafeNumber(item.id) === parentId);
        if (parentIdx === -1) return { status: 404, message: 'Parent comment not found.' };
        const reply = {
          id: Date.now(),
          parentId,
          account: body.account || '',
          name: body.name || '用户',
          text: body.text || '',
          avatar: body.avatar || '',
          likeCount: toSafeNumber(body.likeCount || body.likess, 0),
          location: body.location || '',
          date: body.date || '',
          replyToName: body.replyToName || '',
          replyToAccount: body.replyToAccount || '',
        };
        oldComments[parentIdx].replies = Array.isArray(oldComments[parentIdx].replies) ? oldComments[parentIdx].replies : [];
        oldComments[parentIdx].replies.push(reply);
        const updateReplyRes: any = await this.db.query(`UPDATE \`${table}\` SET \`comment\` = ? WHERE \`id\` = ?;`, [JSON.stringify(oldComments), noteId]);
        const affected = updateReplyRes[0]?.affectedRows ?? updateReplyRes.affectedRows ?? 0;
        if (affected === 1) return { status: 200, message: 'Reply added.', result: { parentId, reply } };
        return { status: 404, message: 'Reply insert failed.' };
      }
      const newComment = {
        id: Date.now(),
        account: body.account || '',
        name: body.name || '用户',
        text: body.text || '',
        avatar: body.avatar || '',
        likeCount: toSafeNumber(body.likeCount || body.likess, 0),
        likeUsers: [],
        location: body.location || '',
        date: body.date || '',
        replies: [],
      };
      oldComments.push(newComment);
      const updateRes: any = await this.db.query(`UPDATE \`${table}\` SET \`comment\` = ? WHERE \`id\` = ?;`, [JSON.stringify(oldComments), noteId]);
      const affected = updateRes[0]?.affectedRows ?? updateRes.affectedRows ?? 0;
      return affected === 1 ? { status: 200, message: 'Comment added.', result: newComment } : { status: 404, message: 'Comment insert failed.' };
    } catch (err: any) {
      return { status: 500, message: 'Add comment failed', error: err.toString() };
    }
  }
}
