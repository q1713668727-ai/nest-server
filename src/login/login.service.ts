import { Injectable } from '@nestjs/common';
import { mkdir, writeFile } from 'fs/promises';
import { AuthTokenService } from '../auth/auth-token.service';
import { getSafeAccountPath } from '../common/content.utils';
import { DbService } from '../db/db.service';

@Injectable()
export class LoginService {
  private readonly avatarChunks = new Map<string, any[]>();

  constructor(
    private readonly db: DbService,
    private readonly authTokenService: AuthTokenService,
  ) {
    this.authTokenService.ensureTokenColumns().catch((err) => {
      console.error('[login] init token columns failed:', err);
    });
  }

  async login(req: any, body: any) {
    try {
      const requestToken = this.authTokenService.extractToken(req);
      if (requestToken) {
        const tokenCheck = await this.authTokenService.verifyToken(requestToken);
        if (tokenCheck.ok) {
          const users = await this.db.query<any>('SELECT * FROM `login` WHERE `account` = ? LIMIT 1;', [tokenCheck.account]);
          if (!users.length) return { status: 404, message: 'Account not found.' };
          const userByToken = { ...users[0], password: undefined };
          if (userByToken.avatar) {
            userByToken.url = userByToken.avatar;
            userByToken.avatar = undefined;
          }
          return {
            status: 200,
            message: 'Login success.',
            result: userByToken,
            token: requestToken,
            tokenExpireAt: tokenCheck.expireAt,
          };
        }
      }

      const { account, password } = body;
      const results = await this.db.query<any>('SELECT * FROM `login` WHERE `account` = ? LIMIT 1;', [account]);
      if (!results.length) return { status: 404, message: 'Account not found or password is incorrect.' };
      const user = { ...results[0] };
      if (password !== user.password) return { status: 401, message: 'Password is incorrect.' };
      const tokenData = await this.authTokenService.issueToken(user.account);
      user.password = undefined;
      if (user.avatar) {
        user.url = user.avatar;
        user.avatar = undefined;
      }
      return { status: 200, message: 'Login success.', result: user, token: tokenData.token, tokenExpireAt: tokenData.expireAt };
    } catch (err: any) {
      return { status: 500, message: 'Login failed', error: err.toString() };
    }
  }

  async logout(req: any) {
    try {
      const token = this.authTokenService.extractToken(req);
      if (!token) return { status: 200, message: 'Logged out.' };
      const check = await this.authTokenService.verifyToken(token);
      if (check.ok && check.account) await this.authTokenService.clearToken(check.account);
      return { status: 200, message: 'Logged out.' };
    } catch (err: any) {
      return { status: 500, message: 'Logout failed', error: err.toString() };
    }
  }

  async reg(body: any) {
    try {
      const result: any = await this.db.query('INSERT INTO `login`(`avatar`,`name`,`account`,`password`,`email`) VALUES(?,?,?,?,?)', [
        body.path,
        body.name,
        body.account,
        body.password,
        body.email,
      ]);
      const affected = result[0]?.affectedRows ?? result.affectedRows ?? 0;
      return affected === 1 ? { status: 200, message: 'Register success.' } : { status: 404, message: 'Register failed.' };
    } catch (err: any) {
      if (err?.code === 'ER_DUP_ENTRY') return { status: 409, message: 'Account or email already exists.' };
      return { status: 500, message: 'Register failed', error: err.toString() };
    }
  }

  regAvatar(body: any) {
    const { account, data } = body;
    if (!account || !data || data.hash === undefined) return { status: 400, message: 'Missing avatar chunk data.' };
    if (!this.avatarChunks.has(account)) this.avatarChunks.set(account, []);
    this.avatarChunks.get(account)![data.hash] = body;
    return { status: 200, message: 'Avatar chunk uploaded.' };
  }

  async regAvatarEnd(body: any) {
    try {
      const { account, type } = body;
      const safeAccount = getSafeAccountPath(account);
      if (!safeAccount) return { status: 400, message: 'Invalid account.' };
      const users = await this.db.query<any>('SELECT account FROM `login` WHERE `account` = ? LIMIT 1;', [account]);
      if (users.length >= 1 && !type) return { status: 409, message: 'Account already exists.' };
      const userChunks = this.avatarChunks.get(account) || [];
      if (!userChunks.length) return { status: 400, message: 'No avatar chunks received.' };
      let file = '';
      for (let i = 0; i < userChunks.length; i += 1) {
        if (!userChunks[i]) {
          this.avatarChunks.delete(account);
          return { status: 400, message: `Avatar chunk ${i} is missing.` };
        }
        file += userChunks[i].data.chunk;
      }
      this.avatarChunks.delete(account);
      const dirPath = `public/user-avatar/${safeAccount}`;
      const randomSuffix = Math.floor(Math.random() * 1000);
      const filePath = `user-avatar/${safeAccount}/${Date.now()}_${randomSuffix}.jpg`;
      const base64 = file.replace(/^data:image\/\w+;base64,/, '');
      const dataBuffer = Buffer.from(base64, 'base64');
      await mkdir(dirPath, { recursive: true });
      await writeFile(`public/${filePath}`, dataBuffer);
      if (type === 'set') {
        const updateRes: any = await this.db.query('UPDATE `login` SET avatar = ? WHERE `account` = ?;', [filePath, account]);
        const affected = updateRes[0]?.affectedRows ?? updateRes.affectedRows ?? 0;
        return affected === 1 ? { status: 200, message: 'Avatar updated.', result: { path: filePath } } : { status: 400, message: 'Avatar update failed.' };
      }
      return { status: 200, message: 'Avatar saved.', result: { path: filePath } };
    } catch (err: any) {
      return { status: 500, message: 'Avatar processing failed', error: err.toString() };
    }
  }
}
