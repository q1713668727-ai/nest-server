import { Injectable, Logger, OnModuleDestroy, OnModuleInit } from '@nestjs/common';
import { ConfigService } from '@nestjs/config'; // 引入 ConfigService
import { createPool, Pool, PoolConnection } from 'mysql2';

@Injectable()
export class DbService implements OnModuleDestroy, OnModuleInit {
  private readonly logger = new Logger(DbService.name);
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
    this.pool.on('connection', (connection) => {
      connection.query("SET NAMES utf8mb4 COLLATE utf8mb4_unicode_ci;");
    });
  }

  async onModuleInit() {
    await this.autoFixMsgTableUtf8mb4();
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

  async transaction<T>(work: (query: <R = any>(sql: string, params?: any[]) => Promise<R>) => Promise<T>): Promise<T> {
    const connection = await this.getConnection();
    const query = <R = any>(sql: string, params: any[] = []) =>
      new Promise<R>((resolve, reject) => {
        connection.query(sql, params, (queryErr, results) => {
          if (queryErr) reject(queryErr);
          else resolve(results as R);
        });
      });

    try {
      await this.beginTransaction(connection);
      const result = await work(query);
      await this.commit(connection);
      return result;
    } catch (error) {
      await this.rollback(connection);
      throw error;
    } finally {
      connection.release();
    }
  }

  private getConnection() {
    return new Promise<PoolConnection>((resolve, reject) => {
      this.pool.getConnection((err, connection) => {
        if (err) reject(err);
        else resolve(connection);
      });
    });
  }

  private beginTransaction(connection: PoolConnection) {
    return new Promise<void>((resolve, reject) => {
      connection.beginTransaction((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private commit(connection: PoolConnection) {
    return new Promise<void>((resolve, reject) => {
      connection.commit((err) => {
        if (err) reject(err);
        else resolve();
      });
    });
  }

  private rollback(connection: PoolConnection) {
    return new Promise<void>((resolve) => {
      connection.rollback(() => resolve());
    });
  }

  async onModuleDestroy() {
    await new Promise<void>((resolve) => {
      this.pool.end(() => resolve());
    });
  }

  private async autoFixMsgTableUtf8mb4() {
    const enabled = String(this.configService.get('DB_AUTO_FIX_UTF8MB4') ?? 'true').toLowerCase() !== 'false';
    if (!enabled) return;

    try {
      const tableRows = await this.query<{ count: number }>(
        `SELECT COUNT(1) AS count
         FROM information_schema.tables
         WHERE table_schema = DATABASE()
           AND table_name = 'msg';`,
      );
      if (!Number(tableRows?.[0]?.count || 0)) return;

      await this.query(`ALTER TABLE \`msg\` CONVERT TO CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;`);

      const columnRows = await this.query<{ isNullable: 'YES' | 'NO'; collationName: string | null }>(
        `SELECT
            IS_NULLABLE AS isNullable,
            COLLATION_NAME AS collationName
         FROM information_schema.columns
         WHERE table_schema = DATABASE()
           AND table_name = 'msg'
           AND column_name = 'message'
         LIMIT 1;`,
      );
      if (!columnRows.length) return;

      const nullableSql = columnRows[0].isNullable === 'YES' ? 'NULL' : 'NOT NULL';
      await this.query(
        `ALTER TABLE \`msg\`
         MODIFY COLUMN \`message\` LONGTEXT
         CHARACTER SET utf8mb4
         COLLATE utf8mb4_unicode_ci
         ${nullableSql};`,
      );

      if (columnRows[0].collationName !== 'utf8mb4_unicode_ci') {
        this.logger.log('Auto-fixed msg.message to utf8mb4_unicode_ci for emoji compatibility.');
      }
    } catch (error) {
      const text = error instanceof Error ? error.message : String(error);
      this.logger.warn(`Auto-fix utf8mb4 for msg.message skipped: ${text}`);
    }
  }
}
