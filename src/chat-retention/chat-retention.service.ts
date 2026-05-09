import {
  Injectable,
  Logger,
  OnModuleDestroy,
  OnModuleInit,
} from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { DbService } from '../db/db.service';

const CHAT_RETENTION_DAYS = 30;
const CHAT_RETENTION_MS = CHAT_RETENTION_DAYS * 24 * 60 * 60 * 1000;
const CLEANUP_INTERVAL_MS = 24 * 60 * 60 * 1000;
const CLEANUP_THROTTLE_MS = 60 * 60 * 1000;

type MsgRow = {
  UserToUser: string;
  message: string | null;
};

type PruneResult<T> = {
  items: T[];
  changed: boolean;
};

type ChatHistoryMessage = Record<string, unknown>;
type ConversationPayload = {
  historyMessage?: ChatHistoryMessage[];
  read?: number;
  [key: string]: unknown;
};
type DbWriteResult =
  | { affectedRows?: number }
  | Array<{ affectedRows?: number }>;

@Injectable()
export class ChatRetentionService implements OnModuleInit, OnModuleDestroy {
  private readonly logger = new Logger(ChatRetentionService.name);
  private readonly marketDb: string;
  private timer: ReturnType<typeof setInterval> | null = null;
  private lastPrivateCleanupAt = 0;
  private lastServiceCleanupAt = 0;

  constructor(
    private readonly db: DbService,
    configService: ConfigService,
  ) {
    const configured = String(
      configService.get('MARKET_DB_NAME') ||
        configService.get('PRODUCT_DB_NAME') ||
        'backstage_server',
    ).trim();
    this.marketDb = /^[a-zA-Z0-9_]+$/.test(configured)
      ? configured
      : 'backstage_server';
  }

  onModuleInit() {
    void this.cleanupAll({ force: true });
    this.timer = setInterval(() => {
      void this.cleanupAll({ force: true });
    }, CLEANUP_INTERVAL_MS);
  }

  onModuleDestroy() {
    if (this.timer) {
      clearInterval(this.timer);
      this.timer = null;
    }
  }

  cutoffTime() {
    return Date.now() - CHAT_RETENTION_MS;
  }

  pruneHistoryMessages<T extends ChatHistoryMessage>(
    messages: T[] | undefined | null,
    cutoff = this.cutoffTime(),
  ): PruneResult<T> {
    const list = Array.isArray(messages) ? messages : [];
    const items = list.filter((message) => {
      const time = this.messageTime(message);
      return !time || time >= cutoff;
    });
    return {
      items,
      changed: items.length !== list.length,
    };
  }

  async cleanupAll(options?: { force?: boolean }) {
    await Promise.all([
      this.cleanupExpiredPrivateConversations(options),
      this.cleanupExpiredServiceMessages(options),
    ]);
  }

  async cleanupExpiredPrivateConversations(options?: { force?: boolean }) {
    const now = Date.now();
    if (
      !options?.force &&
      now - this.lastPrivateCleanupAt < CLEANUP_THROTTLE_MS
    )
      return;
    this.lastPrivateCleanupAt = now;

    try {
      const exists = await this.tableExists(undefined, 'msg');
      if (!exists) return;

      const rows = await this.db.query<MsgRow>(
        'SELECT `UserToUser`, `message` FROM `msg`;',
      );
      let changedRows = 0;
      for (const row of rows) {
        let messageObj: ConversationPayload;
        try {
          const parsed = JSON.parse(row.message || '{}') as unknown;
          if (!this.isRecord(parsed)) continue;
          messageObj = parsed as ConversationPayload;
        } catch {
          continue;
        }

        const pruned = this.pruneHistoryMessages(messageObj.historyMessage);
        if (!pruned.changed) continue;

        messageObj.historyMessage = pruned.items;
        messageObj.read = Math.min(
          Math.max(0, Number(messageObj.read || 0)),
          this.countUnreadMessages(pruned.items),
        );
        await this.db.query(
          'UPDATE `msg` SET `message` = ? WHERE `UserToUser` = ?;',
          [JSON.stringify(messageObj), row.UserToUser],
        );
        changedRows += 1;
      }
      if (changedRows)
        this.logger.log(
          `Pruned expired private chat history in ${changedRows} conversation row(s).`,
        );
    } catch (error) {
      this.logger.warn(
        `Private chat retention cleanup skipped: ${this.errorText(error)}`,
      );
    }
  }

  async cleanupExpiredServiceMessages(options?: { force?: boolean }) {
    const now = Date.now();
    if (
      !options?.force &&
      now - this.lastServiceCleanupAt < CLEANUP_THROTTLE_MS
    )
      return;
    this.lastServiceCleanupAt = now;

    try {
      const exists = await this.tableExists(
        this.marketDb,
        'market_service_messages',
      );
      if (!exists) return;
      const result = (await this.db.query(
        `DELETE FROM ${this.table('market_service_messages')}
         WHERE created_at < DATE_SUB(NOW(), INTERVAL ? DAY);`,
        [CHAT_RETENTION_DAYS],
      )) as unknown;
      const affected = this.affectedRows(result);
      if (affected)
        this.logger.log(
          `Deleted ${affected} expired market service message(s).`,
        );
    } catch (error) {
      this.logger.warn(
        `Market service chat retention cleanup skipped: ${this.errorText(error)}`,
      );
    }
  }

  private countUnreadMessages(messages: ChatHistoryMessage[]) {
    return messages.reduce(
      (count, message) => count + (message?.mine === false ? 1 : 0),
      0,
    );
  }

  private messageTime(message: ChatHistoryMessage) {
    const value =
      message.date ??
      message.time ??
      message.lastTime ??
      message.updateTime ??
      message.updatedAt ??
      message.createTime ??
      message.createdAt;
    return this.parseTime(value);
  }

  private parseTime(value: unknown) {
    if (typeof value === 'number' && Number.isFinite(value))
      return value < 10000000000 ? value * 1000 : value;
    if (value instanceof Date) return value.getTime();
    if (typeof value !== 'string') return 0;
    const raw = value.trim();
    if (!raw) return 0;
    if (/^\d+$/.test(raw)) {
      const numeric = Number(raw);
      return Number.isFinite(numeric)
        ? numeric < 10000000000
          ? numeric * 1000
          : numeric
        : 0;
    }
    const date = new Date(raw);
    return Number.isNaN(date.getTime()) ? 0 : date.getTime();
  }

  private async tableExists(schema: string | undefined, table: string) {
    let schemaName = schema;
    if (!schemaName) {
      const currentRows = await this.db.query<{ dbName: string }>(
        'SELECT DATABASE() AS dbName;',
      );
      schemaName = currentRows[0]?.dbName;
      if (!schemaName) return false;
    }
    const rows = await this.db.query<{ count: number }>(
      `SELECT COUNT(1) AS count
       FROM information_schema.tables
       WHERE table_schema = ?
         AND table_name = ?;`,
      [schemaName, table],
    );
    return Number(rows[0]?.count || 0) > 0;
  }

  private table(name: string) {
    return `\`${this.marketDb}\`.\`${name}\``;
  }

  private errorText(error: unknown) {
    return error instanceof Error ? error.message : String(error);
  }

  private isRecord(value: unknown): value is Record<string, unknown> {
    return Boolean(value && typeof value === 'object' && !Array.isArray(value));
  }

  private affectedRows(value: unknown) {
    const result = value as DbWriteResult;
    return Array.isArray(result)
      ? (result[0]?.affectedRows ?? 0)
      : (result.affectedRows ?? 0);
  }
}
