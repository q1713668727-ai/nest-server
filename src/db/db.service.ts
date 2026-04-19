import { Injectable, OnModuleDestroy } from '@nestjs/common';
import { ConfigService } from '@nestjs/config'; // 引入 ConfigService
import { createPool, Pool } from 'mysql2';

@Injectable()
export class DbService implements OnModuleDestroy {
  private pool: Pool; // 先声明，不立即赋值

  constructor(private configService: ConfigService) {
    // 在构造函数里初始化，此时 ConfigService 已经准备好了
    this.pool = createPool({
      connectionLimit: Number(this.configService.get('DB_CONNECTION_LIMIT') || 10),
      host: this.configService.get('DB_HOST') || '127.0.0.1',
      user: this.configService.get('DB_USER') || 'root',
      port: Number(this.configService.get('DB_PORT') || 3306),
      password: this.configService.get('DB_PASSWORD'), // 安全读取
      database: this.configService.get('DB_NAME') || 'server',
      charset: 'utf8mb4',
    });
  }

  query<T = any>(sql: string, params: any[] = []): Promise<T[]> {
    return new Promise((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) return reject(err);
        connection.query(sql, params, (queryErr, results) => {
          connection.release();
          if (queryErr) reject(queryErr);
          else resolve(results as T[]);
        });
      });
    });
  }

  async onModuleDestroy() {
    await new Promise<void>((resolve) => {
      this.pool.end(() => resolve());
    });
  }
}