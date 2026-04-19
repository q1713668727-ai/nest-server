import { Injectable } from '@nestjs/common';
import { mkdir, writeFile } from 'fs/promises';
import {
  getSafeAccountPath,
  listToText,
  normalizeNoteItem,
  normalizeVideoItem,
  parseAccountList,
  parseContentRefs,
} from '../common/content.utils';
import { DbService } from '../db/db.service';

@Injectable()
export class UserService {
  private readonly bgChunks = new Map<string, any[]>();
  private ensureFollowColumnsTask: Promise<void> | null = null;
  private readonly collectColumnCache = new Map<string, string | null>();

  constructor(private readonly db: DbService) {}

  private getCurrentAccount(req: any, body: any) {
    return String((req.auth && req.auth.account) || body.account || '').trim();
  }

  private relationOf(myFollowingSet: Set<string>, myFollowerSet: Set<string>, targetAccount: string) {
    const followed = myFollowingSet.has(targetAccount);
    const fan = myFollowerSet.has(targetAccount);
    return { followed, fan, mutual: followed && fan };
  }

  private pickPublicUser(user: any) {
    return {
      account: user.account,
      name: user.name || user.account,
      avatar: user.avatar || '',
      url: user.avatar || '',
      attention: Number(user.attention || 0),
      fans: Number(user.fans || 0),
    };
  }

  private async resolveCollectColumn(table: string) {
    if (this.collectColumnCache.has(table)) return this.collectColumnCache.get(table)!;
    const columns = await this.db.query<any>(`SHOW COLUMNS FROM \`${table}\`;`);
    const names = new Set(columns.map((item) => item.Field));
    const column = names.has('collects') ? 'collects' : names.has('collect') ? 'collect' : null;
    this.collectColumnCache.set(table, column);
    return column;
  }

  private async ensureFollowColumns() {
    if (this.ensureFollowColumnsTask) return this.ensureFollowColumnsTask;
    this.ensureFollowColumnsTask = (async () => {
      const columns = await this.db.query<any>('SHOW COLUMNS FROM `login`;');
      const names = new Set(columns.map((item) => item.Field));
      const alters: string[] = [];
      if (!names.has('following_accounts')) alters.push('ADD COLUMN `following_accounts` TEXT NULL');
      if (!names.has('follower_accounts')) alters.push('ADD COLUMN `follower_accounts` TEXT NULL');
      if (alters.length) await this.db.query(`ALTER TABLE \`login\` ${alters.join(', ')};`);
    })().catch((err) => {
      this.ensureFollowColumnsTask = null;
      throw err;
    });
    return this.ensureFollowColumnsTask;
  }

  private async syncUserFollowCount(account: string, followingList: string[], followerList: string[]) {
    await this.db.query(
      'UPDATE `login` SET `following_accounts` = ?, `follower_accounts` = ?, `attention` = ?, `fans` = ? WHERE `account` = ? LIMIT 1;',
      [listToText(followingList), listToText(followerList), followingList.length, followerList.length, account],
    );
  }

  private async loadUserRefsByField(account: string, field: string, defaultType = 'note') {
    const results = await this.db.query<any>(`SELECT \`${field}\` FROM \`login\` WHERE \`account\` = ? LIMIT 1;`, [account]);
    const rawText = results.length ? results[0][field] : '';
    if (!results.length || !rawText) return [];
    return parseContentRefs(rawText, defaultType);
  }

  private async loadContentByRefs(refs: any[]) {
    if (!Array.isArray(refs) || refs.length === 0) return [];
    const noteIds = refs.filter((item) => item.contentType === 'note').map((item) => item.id);
    const videoIds = refs.filter((item) => item.contentType === 'video').map((item) => item.id);
    const [noteRows, videoRows] = await Promise.all([
      noteIds.length ? this.db.query<any>(`SELECT * FROM \`note\` WHERE \`id\` IN (${noteIds.map(() => '?').join(',')});`, noteIds) : Promise.resolve([]),
      videoIds.length
        ? this.db.query<any>(
            `SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM \`video\` v LEFT JOIN \`login\` l ON l.account = v.account WHERE v.\`id\` IN (${videoIds.map(() => '?').join(',')});`,
            videoIds,
          )
        : Promise.resolve([]),
    ]);
    const noteMap = new Map(noteRows.map((item: any) => [item.id, normalizeNoteItem(item)]));
    const videoMap = new Map(videoRows.map((item: any) => [item.id, normalizeVideoItem(item)]));
    return refs.map((ref) => (ref.contentType === 'video' ? videoMap.get(ref.id) : noteMap.get(ref.id))).filter(Boolean);
  }

  async getUserInfo(body: any) {
    try {
      await this.ensureFollowColumns();
      if (!body.account) return { status: 400, message: 'Account is required.' };
      const results = await this.db.query<any>('SELECT * FROM `login` WHERE `account` = ? LIMIT 1;', [body.account]);
      if (!results.length) return { status: 404, message: 'User not found.' };
      const user = { ...results[0] };
      const followingList = parseAccountList(user.following_accounts);
      const followerList = parseAccountList(user.follower_accounts);
      user.attention = followingList.length;
      user.fans = followerList.length;
      user.password = undefined;
      if (user.avatar) {
        user.url = user.avatar;
        user.avatar = undefined;
      }
      return { status: 200, message: 'Fetch success.', result: user };
    } catch (err: any) {
      return { status: 500, message: 'Fetch user info failed', error: err.toString() };
    }
  }

  async followStatus(req: any, body: any) {
    try {
      await this.ensureFollowColumns();
      const myAccount = this.getCurrentAccount(req, body);
      const targetAccount = String(body.targetAccount || '').trim();
      if (!myAccount || !targetAccount) return { status: 400, message: 'Missing account.' };
      if (myAccount === targetAccount) return { status: 200, message: 'Self relation.', result: { followed: false, fan: false, mutual: false } };
      const rows = await this.db.query<any>('SELECT `account`,`following_accounts`,`follower_accounts` FROM `login` WHERE `account` IN (?, ?);', [
        myAccount,
        targetAccount,
      ]);
      const myRow = rows.find((item) => item.account === myAccount);
      if (!myRow) return { status: 404, message: 'User not found.' };
      return { status: 200, message: 'Fetch success.', result: this.relationOf(new Set(parseAccountList(myRow.following_accounts)), new Set(parseAccountList(myRow.follower_accounts)), targetAccount) };
    } catch (err: any) {
      return { status: 500, message: 'Fetch follow status failed', error: err.toString() };
    }
  }

  async toggleFollow(req: any, body: any) {
    try {
      await this.ensureFollowColumns();
      const myAccount = this.getCurrentAccount(req, body);
      const targetAccount = String(body.targetAccount || '').trim();
      const action = String(body.action || '').trim();
      if (!myAccount || !targetAccount) return { status: 400, message: 'Missing account.' };
      if (myAccount === targetAccount) return { status: 400, message: 'Cannot follow self.' };
      const rows = await this.db.query<any>(
        'SELECT `account`,`name`,`avatar`,`attention`,`fans`,`following_accounts`,`follower_accounts` FROM `login` WHERE `account` IN (?, ?);',
        [myAccount, targetAccount],
      );
      const myRow = rows.find((item) => item.account === myAccount);
      const targetRow = rows.find((item) => item.account === targetAccount);
      if (!myRow || !targetRow) return { status: 404, message: 'User not found.' };
      const myFollowing = new Set(parseAccountList(myRow.following_accounts));
      const myFollowers = new Set(parseAccountList(myRow.follower_accounts));
      const targetFollowing = new Set(parseAccountList(targetRow.following_accounts));
      const targetFollowers = new Set(parseAccountList(targetRow.follower_accounts));
      const shouldFollow = action ? action === 'follow' : !myFollowing.has(targetAccount);
      if (shouldFollow) {
        myFollowing.add(targetAccount);
        targetFollowers.add(myAccount);
      } else {
        myFollowing.delete(targetAccount);
        targetFollowers.delete(myAccount);
      }
      await this.syncUserFollowCount(myAccount, Array.from(myFollowing), Array.from(myFollowers));
      await this.syncUserFollowCount(targetAccount, Array.from(targetFollowing), Array.from(targetFollowers));
      const relation = this.relationOf(myFollowing, myFollowers, targetAccount);
      return {
        status: 200,
        message: shouldFollow ? 'Followed.' : 'Unfollowed.',
        result: {
          ...relation,
          self: { account: myAccount, attention: myFollowing.size, fans: myFollowers.size },
          target: { account: targetAccount, attention: targetFollowing.size, fans: targetFollowers.size },
        },
      };
    } catch (err: any) {
      return { status: 500, message: 'Toggle follow failed', error: err.toString() };
    }
  }

  async followList(req: any, body: any) {
    try {
      await this.ensureFollowColumns();
      const myAccount = this.getCurrentAccount(req, body);
      const type = String(body.type || 'follow').trim();
      if (!myAccount) return { status: 400, message: 'Missing account.' };
      const rows = await this.db.query<any>('SELECT `account`,`name`,`avatar`,`attention`,`fans`,`following_accounts`,`follower_accounts` FROM `login`;');
      const myRow = rows.find((item) => item.account === myAccount);
      if (!myRow) return { status: 404, message: 'User not found.' };
      const myFollowing = new Set(parseAccountList(myRow.following_accounts));
      const myFollowers = new Set(parseAccountList(myRow.follower_accounts));
      const mutual = new Set([...myFollowing].filter((item) => myFollowers.has(item)));
      const allByAccount = new Map(rows.map((item) => [item.account, item]));
      const pickWithRelation = (account: string) => {
        const row = allByAccount.get(account);
        if (!row) return null;
        return { ...this.pickPublicUser(row), ...this.relationOf(myFollowing, myFollowers, account) };
      };
      let accountList: string[] = [];
      if (type === 'fans') accountList = Array.from(myFollowers);
      else if (type === 'mutual') accountList = Array.from(mutual);
      else if (type === 'recommend') {
        accountList = rows.map((item) => item.account).filter((account) => account !== myAccount && !myFollowing.has(account)).slice(0, 200);
      } else accountList = Array.from(myFollowing);
      const data = accountList.map((account) => pickWithRelation(account)).filter(Boolean);
      return { status: 200, message: 'Fetch success.', result: { type, data, summary: { follow: myFollowing.size, fans: myFollowers.size, mutual: mutual.size } } };
    } catch (err: any) {
      return { status: 500, message: 'Fetch follow list failed', error: err.toString() };
    }
  }

  async addLikeNote(body: any) {
    try {
      const { likesArr, account, num, setId } = body;
      const contentType = String(body.contentType || 'note').trim() === 'video' ? 'video' : 'note';
      const table = contentType === 'video' ? 'video' : 'note';
      const loginRes: any = await this.db.query('UPDATE `login` SET likes = ? WHERE `account` = ?;', [likesArr, account]);
      if ((loginRes[0]?.affectedRows ?? loginRes.affectedRows ?? 0) === 0) return { status: 404, message: 'User not found.' };
      const targetRes: any = await this.db.query(`UPDATE \`${table}\` SET likes = ? WHERE \`id\` = ?;`, [num, setId]);
      if ((targetRes[0]?.affectedRows ?? targetRes.affectedRows ?? 0) === 0) return { status: 404, message: `${contentType} not found.` };
      return { status: 200, message: 'Like updated.' };
    } catch (err: any) {
      return { status: 500, message: 'Like update failed', error: err.toString() };
    }
  }

  async addCollectNote(body: any) {
    try {
      const { collectsArr, account, num, setId } = body;
      const contentType = String(body.contentType || 'note').trim() === 'video' ? 'video' : 'note';
      const table = contentType === 'video' ? 'video' : 'note';
      const loginRes: any = await this.db.query('UPDATE `login` SET collects = ? WHERE `account` = ?;', [collectsArr, account]);
      if ((loginRes[0]?.affectedRows ?? loginRes.affectedRows ?? 0) === 0) return { status: 404, message: 'User not found.' };
      const collectColumn = await this.resolveCollectColumn(table);
      if (collectColumn) {
        const targetRes: any = await this.db.query(`UPDATE \`${table}\` SET \`${collectColumn}\` = ? WHERE \`id\` = ?;`, [num, setId]);
        if ((targetRes[0]?.affectedRows ?? targetRes.affectedRows ?? 0) === 0) return { status: 404, message: `${contentType} not found.` };
      }
      return { status: 200, message: 'Collect updated.' };
    } catch (err: any) {
      return { status: 500, message: 'Collect update failed', error: err.toString() };
    }
  }

  async myNote(body: any) {
    try {
      const [noteRows, videoRows] = await Promise.all([
        this.db.query<any>('SELECT * FROM `note` WHERE `account` = ?;', [body.account]),
        this.db.query<any>(
          'SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM `video` v LEFT JOIN `login` l ON l.account = v.account WHERE v.`account` = ?;',
          [body.account],
        ),
      ]);
      const list = [...noteRows.map((item) => normalizeNoteItem(item)), ...videoRows.map((item) => normalizeVideoItem(item))].sort(
        (a, b) => Number(b.id || 0) - Number(a.id || 0),
      );
      return { status: 200, message: 'Fetch success.', result: { data: list } };
    } catch (err: any) {
      return { status: 500, message: 'Query failed', error: err.toString() };
    }
  }

  async findLikeNote(body: any) {
    try {
      const refs = await this.loadUserRefsByField(body.account, 'likes', 'note');
      if (!refs.length) return { status: 200, message: 'Fetch success.', result: { data: [] } };
      const data = await this.loadContentByRefs(refs);
      return { status: 200, message: 'Fetch success.', result: { data } };
    } catch (err: any) {
      return { status: 500, message: 'Favorite query failed', error: err.toString() };
    }
  }

  async findCollectNote(body: any) {
    try {
      const refs = await this.loadUserRefsByField(body.account, 'collects', 'note');
      if (!refs.length) return { status: 200, message: 'Fetch success.', result: { data: [] } };
      const data = await this.loadContentByRefs(refs);
      return { status: 200, message: 'Fetch success.', result: { data } };
    } catch (err: any) {
      return { status: 500, message: 'Collect query failed', error: err.toString() };
    }
  }

  setBackground(body: any) {
    const { account, data } = body;
    if (!account || !data || data.hash === undefined) return { status: 400, message: 'Missing params.' };
    if (!this.bgChunks.has(account)) this.bgChunks.set(account, []);
    this.bgChunks.get(account)![data.hash] = body;
    return { status: 200, message: 'Background chunk uploaded.' };
  }

  async setBackgroundEnd(body: any) {
    try {
      const { account } = body;
      const safeAccount = getSafeAccountPath(account);
      if (!safeAccount) return { status: 400, message: 'Invalid account.' };
      const userChunks = this.bgChunks.get(account) || [];
      if (!userChunks.length) return { status: 400, message: 'File chunk is incomplete.' };
      let fileBase64 = '';
      for (let i = 0; i < userChunks.length; i += 1) {
        if (!userChunks[i]) return { status: 400, message: 'File chunk is incomplete.' };
        fileBase64 += userChunks[i].data.chunk;
      }
      this.bgChunks.delete(account);
      const base64Data = fileBase64.replace(/^data:image\/\w+;base64,/, '');
      const dataBuffer = Buffer.from(base64Data, 'base64');
      const fileName = `${Date.now()}.jpg`;
      const relativePath = `user-background/${safeAccount}/${fileName}`;
      await mkdir(`public/user-background/${safeAccount}`, { recursive: true });
      await writeFile(`public/${relativePath}`, dataBuffer);
      const updateRes: any = await this.db.query('UPDATE `login` SET background = ? WHERE `account` = ?;', [relativePath, account]);
      return (updateRes[0]?.affectedRows ?? updateRes.affectedRows ?? 0) === 1
        ? { status: 200, message: 'Background updated.', result: { path: relativePath } }
        : { status: 404, message: 'Database update failed.' };
    } catch (err: any) {
      return { status: 500, message: 'Background update failed', error: err.toString() };
    }
  }
}
