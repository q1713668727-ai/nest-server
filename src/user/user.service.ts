import { Injectable } from '@nestjs/common';
import { mkdir, writeFile } from 'fs/promises';
import {
  getSafeAccountPath,
  listToText,
  normalizeNoteItem,
  normalizeLikeUsers,
  normalizeVideoItem,
  parseAccountList,
  parseContentRefs,
  parseNoteComments,
} from '../common/content.utils';
import { DbService } from '../db/db.service';

@Injectable()
export class UserService {
  private readonly bgChunks = new Map<string, any[]>();
  private ensureFollowColumnsTask: Promise<void> | null = null;
  private readonly collectColumnCache = new Map<string, string | null>();
  private readonly hiddenColumnCache = new Set<string>();

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
    if (!names.has('collects') && !names.has('collect')) {
      await this.db.query(`ALTER TABLE \`${table}\` ADD COLUMN \`collect\` INT NOT NULL DEFAULT 0;`);
      this.collectColumnCache.set(table, 'collect');
      return 'collect';
    }
    const column = names.has('collects') ? 'collects' : 'collect';
    this.collectColumnCache.set(table, column);
    return column;
  }

  private async ensureHiddenColumn(table: 'note' | 'video') {
    if (this.hiddenColumnCache.has(table)) return;
    const columns = await this.db.query<any>(`SHOW COLUMNS FROM \`${table}\`;`);
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('hidden')) {
      await this.db.query(`ALTER TABLE \`${table}\` ADD COLUMN \`hidden\` TINYINT(1) NOT NULL DEFAULT 0;`);
    }
    this.hiddenColumnCache.add(table);
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

  private async ensureOrderTable() {
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS \`order\` (
        \`account\` VARCHAR(255) NOT NULL,
        \`market_addresses\` JSON NULL,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (\`account\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    const columns = await this.db.query<any>('SHOW COLUMNS FROM `order`;');
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('market_addresses')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `market_addresses` JSON NULL;');
    }
    if (!names.has('created_at')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;');
    }
    if (!names.has('updated_at')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;');
    }
  }

  private normalizeAddressItems(value: any) {
    let raw = value;
    if (typeof raw === 'string') {
      try {
        raw = JSON.parse(raw || '[]');
      } catch {
        raw = [];
      }
    }
    if (!Array.isArray(raw)) return [];
    return raw
      .map((item) => ({
        id: String(item?.id || '').trim(),
        region: String(item?.region || '').trim(),
        detail: String(item?.detail || '').trim(),
        name: String(item?.name || '').trim(),
        phone: String(item?.phone || '').trim(),
        isDefault: Boolean(item?.isDefault),
        updatedAt: Number(item?.updatedAt || Date.now()),
      }))
      .filter((item) => item.id && item.region && item.detail && item.name && item.phone);
  }

  private normalizeAddressInput(body: any, existingId = '') {
    return {
      id: String(existingId || body?.id || Date.now()).trim(),
      region: String(body?.region || '').trim(),
      detail: String(body?.detail || '').trim(),
      name: String(body?.name || '').trim(),
      phone: String(body?.phone || '').trim(),
      isDefault: Boolean(body?.isDefault),
      updatedAt: Date.now(),
    };
  }

  private async readMarketAddresses(account: string) {
    await this.ensureOrderTable();
    await this.db.query('INSERT IGNORE INTO `order` (`account`, `market_addresses`) VALUES (?, JSON_ARRAY());', [account]);
    const rows = await this.db.query<any>('SELECT `market_addresses` FROM `order` WHERE `account` = ? LIMIT 1;', [account]);
    return this.normalizeAddressItems(rows[0]?.market_addresses);
  }

  private async writeMarketAddresses(account: string, items: any[]) {
    await this.ensureOrderTable();
    await this.db.query(
      `INSERT INTO \`order\` (\`account\`, \`market_addresses\`) VALUES (?, ?)
       ON DUPLICATE KEY UPDATE \`market_addresses\` = VALUES(\`market_addresses\`), \`updated_at\` = CURRENT_TIMESTAMP;`,
      [account, JSON.stringify(items)],
    );
    return items;
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

  private contentThumbnail(contentType: 'note' | 'video', item: any) {
    const account = String(item?.account || '').trim();
    if (contentType === 'video') {
      const image = String(item?.image || item?.cover || '').trim().replace(/^\.\.\//, '');
      if (!image) return '';
      if (image.includes('/')) return image;
      return account ? `video-cover/${account}/${image}` : image;
    }

    const firstImage = String(item?.image || '')
      .split('/')
      .map((part) => part.trim())
      .filter(Boolean)[0];
    return account && firstImage ? `note-image/${account}/${firstImage}` : '';
  }

  private actorFrom(account: string, loginMap: Map<string, any>, fallback?: any) {
    const row = loginMap.get(account);
    return {
      account,
      name: String(fallback?.name || row?.name || account || '用户'),
      avatar: String(fallback?.avatar || row?.avatar || row?.url || ''),
    };
  }

  private interactionDate(...values: any[]) {
    return values.map((value) => String(value || '').trim()).find(Boolean) || '';
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

  async marketAddresses(req: any) {
    try {
      const account = this.getCurrentAccount(req, {});
      if (!account) return { status: 400, message: 'Missing account.', result: [] };
      return { status: 200, message: 'Fetch success.', result: await this.readMarketAddresses(account) };
    } catch (err: any) {
      return { status: 500, message: 'Fetch addresses failed', error: err.toString(), result: [] };
    }
  }

  async saveMarketAddress(req: any, body: any) {
    try {
      const account = this.getCurrentAccount(req, body);
      if (!account) return { status: 400, message: 'Missing account.', result: [] };
      const input = this.normalizeAddressInput(body, body?.id);
      if (!input.region || !input.detail || !input.name || !input.phone) return { status: 400, message: 'Address is incomplete.', result: [] };
      const items = await this.readMarketAddresses(account);
      const exists = items.some((item) => item.id === input.id);
      const next = exists ? items.map((item) => (item.id === input.id ? input : item)) : [...items, input];
      const normalized = input.isDefault ? next.map((item) => ({ ...item, isDefault: item.id === input.id })) : next;
      return { status: 200, message: 'Saved.', result: await this.writeMarketAddresses(account, normalized) };
    } catch (err: any) {
      return { status: 500, message: 'Save address failed', error: err.toString(), result: [] };
    }
  }

  async deleteMarketAddress(req: any, body: any) {
    try {
      const account = this.getCurrentAccount(req, body);
      const id = String(body?.id || '').trim();
      if (!account || !id) return { status: 400, message: 'Missing params.', result: [] };
      const next = (await this.readMarketAddresses(account)).filter((item) => item.id !== id);
      return { status: 200, message: 'Deleted.', result: await this.writeMarketAddresses(account, next) };
    } catch (err: any) {
      return { status: 500, message: 'Delete address failed', error: err.toString(), result: [] };
    }
  }

  async setDefaultMarketAddress(req: any, body: any) {
    try {
      const account = this.getCurrentAccount(req, body);
      const id = String(body?.id || '').trim();
      if (!account || !id) return { status: 400, message: 'Missing params.', result: [] };
      const next = (await this.readMarketAddresses(account)).map((item) => ({ ...item, isDefault: item.id === id }));
      return { status: 200, message: 'Updated.', result: await this.writeMarketAddresses(account, next) };
    } catch (err: any) {
      return { status: 500, message: 'Set default address failed', error: err.toString(), result: [] };
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
      await Promise.all([this.ensureHiddenColumn('note'), this.ensureHiddenColumn('video')]);
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

  async receivedInteractions(req: any, body: any) {
    try {
      const account = this.getCurrentAccount(req, body);
      if (!account) return { status: 400, message: 'Missing account.', result: { data: [] } };

      const [loginRows, noteRows, videoRows] = await Promise.all([
        this.db.query<any>('SELECT `account`,`name`,`avatar`,`likes`,`collects` FROM `login`;'),
        this.db.query<any>('SELECT * FROM `note` WHERE `account` = ?;', [account]),
        this.db.query<any>(
          'SELECT v.*, l.avatar AS authorAvatar, l.name AS authorName FROM `video` v LEFT JOIN `login` l ON l.account = v.account WHERE v.`account` = ?;',
          [account],
        ),
      ]);

      const loginMap = new Map(loginRows.map((item: any) => [String(item.account || ''), item]));
      const contentMap = new Map<string, any>();
      const ownContents = [
        ...noteRows.map((item: any) => ({ ...normalizeNoteItem(item), raw: item })),
        ...videoRows.map((item: any) => ({ ...normalizeVideoItem(item), raw: item })),
      ];
      ownContents.forEach((item: any) => {
        contentMap.set(`${item.contentType}-${item.id}`, item);
      });

      const notices: any[] = [];
      const pushNotice = (notice: any) => {
        const actorAccount = String(notice.actor?.account || '').trim();
        if (!actorAccount || actorAccount === account) return;
        notices.push({
          id: `${notice.kind}-${notice.contentType}-${notice.contentId}-${actorAccount}-${notice.commentId || 0}-${notice.replyId || 0}`,
          ...notice,
        });
      };

      loginRows.forEach((user: any) => {
        const actorAccount = String(user.account || '').trim();
        if (!actorAccount || actorAccount === account) return;
        const actor = this.actorFrom(actorAccount, loginMap);

        parseContentRefs(user.likes || '', 'note').forEach((ref) => {
          const content = contentMap.get(`${ref.contentType}-${ref.id}`);
          if (!content) return;
          pushNotice({
            kind: 'content-like',
            action: `赞了你的${ref.contentType === 'video' ? '视频' : '笔记'}`,
            actor,
            contentType: ref.contentType,
            contentId: String(ref.id),
            quote: String(content.title || content.brief || (ref.contentType === 'video' ? '视频内容' : '笔记内容')),
            thumbnail: this.contentThumbnail(ref.contentType, content.raw || content),
            date: this.interactionDate(content.date),
          });
        });

        parseContentRefs(user.collects || '', 'note').forEach((ref) => {
          const content = contentMap.get(`${ref.contentType}-${ref.id}`);
          if (!content) return;
          pushNotice({
            kind: 'content-collect',
            action: `收藏了你的${ref.contentType === 'video' ? '视频' : '笔记'}`,
            actor,
            contentType: ref.contentType,
            contentId: String(ref.id),
            quote: String(content.title || content.brief || (ref.contentType === 'video' ? '视频内容' : '笔记内容')),
            thumbnail: this.contentThumbnail(ref.contentType, content.raw || content),
            date: this.interactionDate(content.date),
          });
        });
      });

      ownContents.forEach((content: any) => {
        const contentType = content.contentType === 'video' ? 'video' : 'note';
        const raw = content.raw || content;
        const contentSummary = String(content.title || content.brief || (contentType === 'video' ? '视频内容' : '笔记内容'));
        parseNoteComments(raw.comment).forEach((comment: any, commentIndex: number) => {
          const commentId = String(comment?.id ?? commentIndex + 1);
          const commentOwner = String(comment?.account || '').trim();
          if (commentOwner && commentOwner !== account) {
            pushNotice({
              kind: 'comment-received',
              action: `评论了你的${contentType === 'video' ? '视频' : '笔记'}`,
              actor: this.actorFrom(commentOwner, loginMap, comment),
              contentType,
              contentId: String(content.id),
              commentId,
              quote: contentSummary,
              message: String(comment?.text || ''),
              thumbnail: this.contentThumbnail(contentType, raw),
              date: this.interactionDate(comment?.date, content.date),
            });
          }

          if (String(comment?.account || '').trim() === account) {
            const likeUsers = normalizeLikeUsers(comment?.likeUsers || comment?.likeAccounts || comment?.likeUserInfo);
            likeUsers.forEach((user: any) => {
              const actorAccount = String(user?.account || '').trim();
              pushNotice({
                kind: 'comment-like',
                action: '赞了你的评论',
                actor: this.actorFrom(actorAccount, loginMap, user),
                contentType,
                contentId: String(content.id),
                commentId,
                quote: String(comment?.text || ''),
                thumbnail: this.contentThumbnail(contentType, raw),
                date: this.interactionDate(comment?.date, content.date),
              });
            });
          }

          const replies = Array.isArray(comment?.replies) ? comment.replies : [];
          replies.forEach((reply: any, replyIndex: number) => {
            const replyId = String(reply?.id ?? replyIndex + 1);
            const replyOwner = String(reply?.account || '').trim();
            const replyToAccount = String(reply?.replyToAccount || '').trim();
            const shouldNotifyReply =
              replyOwner &&
              replyOwner !== account &&
              (replyToAccount === account || (!replyToAccount && commentOwner === account));
            if (shouldNotifyReply) {
              pushNotice({
                kind: 'reply-received',
                action: '回复了你的评论',
                actor: this.actorFrom(replyOwner, loginMap, reply),
                contentType,
                contentId: String(content.id),
                commentId,
                replyId,
                quote: contentSummary,
                message: String(reply?.text || ''),
                thumbnail: this.contentThumbnail(contentType, raw),
                date: this.interactionDate(reply?.date, comment?.date, content.date),
              });
            }

            if (String(reply?.account || '').trim() !== account) return;
            const likeUsers = normalizeLikeUsers(reply?.likeUsers || reply?.likeAccounts || reply?.likeUserInfo);
            likeUsers.forEach((user: any) => {
              const actorAccount = String(user?.account || '').trim();
              pushNotice({
                kind: 'reply-like',
                action: '赞了你的回复',
                actor: this.actorFrom(actorAccount, loginMap, user),
                contentType,
                contentId: String(content.id),
                commentId,
                replyId,
                quote: String(reply?.text || ''),
                thumbnail: this.contentThumbnail(contentType, raw),
                date: this.interactionDate(reply?.date, comment?.date, content.date),
              });
            });
          });
        });
      });

      // 回复我的评论/回复（包括我在他人内容下的评论）
      // 这一段作为增强能力，单独降级，避免扫描异常影响主流程。
      try {
        const [noteCommentRows, videoCommentRows] = await Promise.all([
          this.db.query<any>('SELECT * FROM `note` WHERE `comment` IS NOT NULL AND LENGTH(CAST(`comment` AS CHAR)) > 2;'),
          this.db.query<any>('SELECT * FROM `video` WHERE `comment` IS NOT NULL AND LENGTH(CAST(`comment` AS CHAR)) > 2;'),
        ]);
        const allCommentContents = [
          ...noteCommentRows.map((item: any) => ({ ...normalizeNoteItem(item), raw: item })),
          ...videoCommentRows.map((item: any) => ({ ...normalizeVideoItem(item), raw: item })),
        ];
        allCommentContents.forEach((content: any) => {
          const contentType = content.contentType === 'video' ? 'video' : 'note';
          const raw = content.raw || content;
          const contentSummary = String(content.title || content.brief || (contentType === 'video' ? '视频内容' : '笔记内容'));
          parseNoteComments(raw.comment).forEach((comment: any, commentIndex: number) => {
            const commentId = String(comment?.id ?? commentIndex + 1);
            const commentOwner = String(comment?.account || '').trim();
            const replies = Array.isArray(comment?.replies) ? comment.replies : [];
            replies.forEach((reply: any, replyIndex: number) => {
              const replyOwner = String(reply?.account || '').trim();
              const replyToAccount = String(reply?.replyToAccount || '').trim();
              if (!replyOwner || replyOwner === account) return;
              const replyToMe = replyToAccount === account || (!replyToAccount && commentOwner === account);
              if (!replyToMe) return;
              pushNotice({
                kind: 'reply-received',
                action: '回复了你的评论',
                actor: this.actorFrom(replyOwner, loginMap, reply),
                contentType,
                contentId: String(content.id),
                commentId,
                replyId: String(reply?.id ?? replyIndex + 1),
                quote: contentSummary,
                message: String(reply?.text || ''),
                thumbnail: this.contentThumbnail(contentType, raw),
                date: this.interactionDate(reply?.date, comment?.date, content.date),
              });
            });
          });
        });
      } catch {
        // ignore scan error
      }

      notices.sort((a, b) => {
        const timeA = new Date(String(a.date || '').replace(/-/g, '/')).getTime();
        const timeB = new Date(String(b.date || '').replace(/-/g, '/')).getTime();
        const safeA = Number.isFinite(timeA) ? timeA : 0;
        const safeB = Number.isFinite(timeB) ? timeB : 0;
        if (safeB !== safeA) return safeB - safeA;
        return Number(b.contentId || 0) - Number(a.contentId || 0);
      });
      return { status: 200, message: 'Fetch success.', result: { data: notices.slice(0, 200) } };
    } catch (err: any) {
      // 兼容旧库结构，失败时降级返回空列表，避免前端进入错误态。
      return { status: 200, message: 'Fetch success.', error: err.toString(), result: { data: [] } };
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
