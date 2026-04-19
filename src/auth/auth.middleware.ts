import { Injectable, NestMiddleware } from '@nestjs/common';
import { NextFunction, Request, Response } from 'express';
import { AuthTokenService } from './auth-token.service';

@Injectable()
export class AuthMiddleware implements NestMiddleware {
  constructor(private readonly authTokenService: AuthTokenService) {}

  async use(req: Request & { auth?: any }, res: Response, next: NextFunction) {
    try {
      if (req.method === 'OPTIONS') {
        next();
        return;
      }
      const token = this.authTokenService.extractToken(req);
      if (!token) {
        res.status(401).send({ status: 401, code: 'TOKEN_REQUIRED', message: '登录已失效，请先登录。' });
        return;
      }
      const check = await this.authTokenService.verifyToken(token);
      if (!check.ok) {
        res.status(401).send({ status: 401, code: check.code, message: check.message });
        return;
      }
      req.auth = { account: check.account, tokenExpireAt: check.expireAt };
      next();
    } catch (err) {
      console.error('[auth] verify failed:', err);
      res.status(500).send({ status: 500, code: 'AUTH_VERIFY_ERROR', message: '鉴权失败，请稍后重试。' });
    }
  }
}
