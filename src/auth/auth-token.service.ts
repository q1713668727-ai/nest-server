import { Injectable } from '@nestjs/common';
import { randomBytes } from 'crypto';
import { DbService } from '../db/db.service';

const TOKEN_TTL_MS = 7 * 24 * 60 * 60 * 1000;

@Injectable()
export class AuthTokenService {
  private ensureColumnsTask: Promise<void> | null = null;

  constructor(private readonly db: DbService) {}

  async ensureTokenColumns() {
    if (this.ensureColumnsTask) return this.ensureColumnsTask;
    this.ensureColumnsTask = (async () => {
      const columns = await this.db.query<any>('SHOW COLUMNS FROM `login`;');
      const names = new Set(columns.map((item) => item.Field));
      const alters: string[] = [];
      if (!names.has('auth_token')) alters.push('ADD COLUMN `auth_token` VARCHAR(128) NULL');
      if (!names.has('auth_token_expire_at')) alters.push('ADD COLUMN `auth_token_expire_at` BIGINT NULL');
      if (alters.length) {
        await this.db.query(`ALTER TABLE \`login\` ${alters.join(', ')};`);
      }
    })().catch((err) => {
      this.ensureColumnsTask = null;
      throw err;
    });
    return this.ensureColumnsTask;
  }

  private buildToken(account: string) {
    return `${encodeURIComponent(account)}.${randomBytes(32).toString('hex')}`;
  }

  private parseAccountFromToken(token: string) {
    if (!token) return '';
    const idx = token.indexOf('.');
    if (idx <= 0) return '';
    try {
      return decodeURIComponent(token.slice(0, idx));
    } catch {
      return '';
    }
  }

  extractToken(req: any) {
    const authHeader = req.headers?.authorization || req.headers?.Authorization;
    if (typeof authHeader === 'string' && authHeader.trim()) {
      const text = authHeader.trim();
      if (/^Bearer\s+/i.test(text)) return text.replace(/^Bearer\s+/i, '').trim();
      return text;
    }
    if (typeof req.body?.token === 'string' && req.body.token.trim()) return req.body.token.trim();
    if (typeof req.query?.token === 'string' && req.query.token.trim()) return req.query.token.trim();
    return '';
  }

  async issueToken(account: string) {
    const clean = String(account || '').trim();
    if (!clean) throw new Error('Invalid account.');
    await this.ensureTokenColumns();
    const token = this.buildToken(clean);
    const expireAt = Date.now() + TOKEN_TTL_MS;
    await this.db.query(
      'UPDATE `login` SET `auth_token` = ?, `auth_token_expire_at` = ? WHERE `account` = ? LIMIT 1;',
      [token, expireAt, clean],
    );
    return { token, expireAt };
  }

  async verifyToken(token: string) {
    await this.ensureTokenColumns();
    const account = this.parseAccountFromToken(token);
    if (!account) return { ok: false, code: 'TOKEN_INVALID', message: '无效登录令牌，请重新登录。' };
    const users = await this.db.query<any>(
      'SELECT `account`, `auth_token`, `auth_token_expire_at` FROM `login` WHERE `account` = ? LIMIT 1;',
      [account],
    );
    if (!users.length) return { ok: false, code: 'TOKEN_INVALID', message: '账号不存在，请重新登录。' };
    const user = users[0];
    if (!user.auth_token) return { ok: false, code: 'TOKEN_INVALID', message: '登录状态已失效，请重新登录。' };
    if (String(user.auth_token) !== String(token)) {
      return { ok: false, code: 'TOKEN_KICKED_OUT', message: '账号已在其他设备登录，当前设备已下线。' };
    }
    const expireAt = Number(user.auth_token_expire_at || 0);
    if (!expireAt || expireAt <= Date.now()) {
      await this.clearToken(account);
      return { ok: false, code: 'TOKEN_EXPIRED', message: '登录已过期，请重新登录。' };
    }
    return { ok: true, account, expireAt };
  }

  async clearToken(account: string) {
    const clean = String(account || '').trim();
    if (!clean) return;
    await this.ensureTokenColumns();
    await this.db.query(
      'UPDATE `login` SET `auth_token` = NULL, `auth_token_expire_at` = NULL WHERE `account` = ? LIMIT 1;',
      [clean],
    );
  }
}
