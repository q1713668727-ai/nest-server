import { InjectQueue } from '@nestjs/bullmq';
import { Injectable } from '@nestjs/common';
import { ConfigService } from '@nestjs/config';
import { Queue } from 'bullmq';
import { AuthTokenService } from '../auth/auth-token.service';
import { DbService } from '../db/db.service';

export const MARKET_ORDER_QUEUE = 'market-order';
const ORDER_PAY_TIMEOUT_MS = 30 * 60 * 1000;

type CategoryRow = {
  id: number;
  parent_id: number | null;
  category_key: string;
  name: string;
  icon_url: string | null;
  level: number;
  sort_order: number;
  feature_titles: string | null;
};

type CategoryChildRow = {
  id: number;
  parent_id: number;
  category_key: string;
  name: string;
  icon_url: string | null;
  sort_order: number;
};

type ProductRow = {
  id: number;
  spu_code: string;
  shop_id: number | null;
  category_id: number | null;
  category_name: string | null;
  category_path: string | null;
  shop_name: string | null;
  shop_avatar_url: string | null;
  service_level: string | null;
  fans_count: number | null;
  sales_count: number | null;
  rating: string | number | null;
  name: string;
  main_image_url: string | null;
  min_price: string | number;
  max_price: string | number;
  origin_price: string | number | null;
  total_stock: number;
  sold_count: number;
  favorite_count: number;
  is_free_shipping: number;
  status: number;
  detail_json: string | Record<string, any> | null;
};

type SkuRow = {
  id: number;
  sku_code: string;
  specs: string | null;
  image_url: string | null;
  price: string | number;
  origin_price: string | number | null;
  stock: number;
  status: number;
};

type CouponRow = {
  id: number;
  coupon_code: string;
  shop_id: number | null;
  product_id: number | null;
  product_name: string | null;
  title: string;
  threshold_amount: string | number;
  discount_amount: string | number;
  is_stackable: number;
  is_once_per_user: number;
  coupon_level?: 'product' | 'shop' | 'platform' | null;
  receive_mode?: 'once' | 'unlimited' | 'grant_only' | null;
  total_count: number;
  received_count: number;
  end_at: Date | string | null;
  status: number;
};

type MyCouponRow = CouponRow & {
  shop_name: string | null;
  shop_avatar_url: string | null;
  claimed_at: string | number | null;
};

type MarketOrderRow = {
  id: number;
  order_no: string;
  user_account: string | null;
  shop_id: number | null;
  shop_name: string | null;
  shop_avatar_url: string | null;
  receiver_snapshot: string | Record<string, any> | null;
  product_amount: string | number;
  freight_amount: string | number;
  discount_amount: string | number;
  pay_amount: string | number;
  payment_method: string | null;
  remark: string | null;
  coupon_snapshot?: string | Record<string, any> | null;
  refund_status?: number | null;
  refund_reason?: string | null;
  refund_received_status?: string | null;
  refund_applied_at?: Date | string | null;
  refund_reviewed_at?: Date | string | null;
  refund_origin_status?: number | null;
  status: number;
  created_at: Date | string;
  paid_at: Date | string | null;
  shipped_at?: Date | string | null;
  cancelled_at?: Date | string | null;
};

type MarketReviewRow = {
  id: number;
  order_id: number;
  order_item_id: number;
  product_id: number;
  shop_id: number | null;
  user_account: string;
  user_name: string | null;
  user_avatar?: string | null;
  rating: number;
  content: string;
  reply_content: string | null;
  reply_shop_name: string | null;
  replied_at: Date | string | null;
  created_at: Date | string;
};

type MarketServiceSessionRow = {
  id: number;
  session_no: string;
  user_account: string | null;
  shop_id: number | null;
  product_id: number | null;
  shop_name: string | null;
  shop_avatar_url: string | null;
  shop_service_level: string | null;
  shop_sales_count: number | null;
  product_name: string | null;
  product_image_url: string | null;
  product_price: string | number | null;
  status: number;
  unread_count?: number | null;
  created_at: Date | string;
  updated_at: Date | string;
};

type MarketServiceMessageRow = {
  id: number;
  session_id: number;
  sender_type: number;
  message_type?: number | null;
  content: string | null;
  payload: string | Record<string, any> | null;
  created_at: Date | string;
};

type ServiceOrderTag = {
  orderId: number;
  orderNo: string;
  status: string;
  createdAt: number;
  total: number;
};

type ServiceOrderTagRow = {
  id: number;
  order_no: string;
  status: number;
  created_at: Date | string;
  pay_amount: string | number;
};

type ServiceProductPayload = {
  productId?: number;
  name?: string;
  imageUrl?: string;
  price?: number;
  specText?: string;
  orderNo?: string;
  orderStatus?: string;
  orderQuantity?: string;
  orderTotal?: string;
};

type CouponWalletItem = {
  couponId: number;
  claimedAt: number;
  usedAt?: number;
  usedOrderIds?: number[];
};

type MarketOrderItemRow = {
  id: number;
  order_id: number;
  product_id: number | null;
  sku_id: number | null;
  product_snapshot: string | Record<string, any> | null;
  quantity: number;
  unit_price: string | number;
  total_amount: string | number;
};

@Injectable()
export class MarketService {
  private readonly marketDb: string;

  constructor(
    private readonly db: DbService,
    private readonly authTokenService: AuthTokenService,
    @InjectQueue(MARKET_ORDER_QUEUE) private readonly orderQueue: Queue,
    configService: ConfigService,
  ) {
    const configured = String(configService.get('MARKET_DB_NAME') || configService.get('PRODUCT_DB_NAME') || 'backstage_server').trim();
    this.marketDb = /^[a-zA-Z0-9_]+$/.test(configured) ? configured : 'backstage_server';
  }

  private table(name: string) {
    return `\`${this.marketDb}\`.\`${name}\``;
  }

  private parseDetail(row: Pick<ProductRow, 'detail_json'>) {
    if (!row.detail_json) return {};
    if (typeof row.detail_json === 'object') return row.detail_json;
    try {
      return JSON.parse(row.detail_json);
    } catch {
      return {};
    }
  }

  private normalizeImageUrls(value: any) {
    if (!Array.isArray(value)) return [];
    return value.map((item) => String(item || '').trim()).filter(Boolean);
  }

  private normalizeSpecs(value: any): Array<{ name: string; value: string }> {
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
      .map((item) => ({ name: String(item?.name || '').trim(), value: String(item?.value || '').trim() }))
      .filter((item) => item.name || item.value);
  }

  private statusText(status: number) {
    if (status === 2) return '售罄';
    return status === 1 ? '上架' : '下架';
  }

  private soldText(value: number) {
    if (value >= 10000) return `${Number((value / 10000).toFixed(1))}万+`;
    return String(value || 0);
  }

  private categoryPathSql() {
    return `CASE
      WHEN c.level = 1 THEN c.name
      WHEN c.level = 2 THEN CONCAT_WS(' / ', cp.name, c.name)
      ELSE CONCAT_WS(' / ', cgp.name, cp.name, c.name)
    END`;
  }

  private productSelectSql() {
    return `SELECT p.*, c.name AS category_name,
             ${this.categoryPathSql()} AS category_path,
             COALESCE(u.shop_name, u.nickname, u.username, s.username) AS shop_name,
             u.shop_avatar_url,
             s.service_level, s.fans_count, s.sales_count, s.rating
      FROM ${this.table('products')} p
      LEFT JOIN ${this.table('market_categories')} c ON c.id = p.category_id
      LEFT JOIN ${this.table('market_categories')} cp ON cp.id = c.parent_id
      LEFT JOIN ${this.table('market_categories')} cgp ON cgp.id = cp.parent_id
      LEFT JOIN ${this.table('market_shops')} s ON s.id = p.shop_id AND s.deleted_at IS NULL
      LEFT JOIN ${this.table('admin_users')} u ON u.username = s.username`;
  }

  private async getSkuRows(productId: number) {
    return this.db.query<SkuRow>(
      `SELECT id, sku_code, specs, image_url, price, origin_price, stock, status
       FROM ${this.table('skus')}
       WHERE product_id = ?
       ORDER BY id ASC;`,
      [productId],
    );
  }

  private mapProduct(row: ProductRow, skuRows: SkuRow[] = []) {
    const detail = this.parseDetail(row) as any;
    const imageUrls = this.normalizeImageUrls(detail.imageUrls);
    const hdImageUrls = this.normalizeImageUrls(detail.hdImageUrls);
    const mainImageUrl = String(row.main_image_url || '').trim();
    if (mainImageUrl && !imageUrls.includes(mainImageUrl)) imageUrls.unshift(mainImageUrl);

    const skus = skuRows.map((sku) => {
      const specs = this.normalizeSpecs(sku.specs);
      return {
        id: sku.id,
        code: sku.sku_code,
        specs,
        imageUrl: sku.image_url || '',
        price: Number(sku.price || 0),
        originPrice: Number(sku.origin_price || sku.price || 0),
        stock: Number(sku.stock || 0),
        status: this.statusText(Number(sku.status || 0)),
      };
    });
    const firstSku = skus[0];
    const sold = Number(row.sold_count || 0);

    return {
      id: row.id,
      code: row.spu_code,
      name: row.name,
      categoryId: row.category_id,
      category: row.category_name || '',
      categoryPath: row.category_path || row.category_name || '',
      shopId: row.shop_id,
      shop: row.shop_name || '',
      shopAvatarUrl: row.shop_avatar_url || '',
      shopServiceLevel: row.service_level || '金牌客服',
      shopFans: Number(row.fans_count || 0),
      shopSales: Number(row.sales_count || 0),
      shopRating: Number(row.rating || 5),
      price: firstSku?.price ?? Number(row.min_price || 0),
      originPrice: firstSku?.originPrice ?? Number(row.origin_price || row.min_price || 0),
      minPrice: Number(row.min_price || 0),
      maxPrice: Number(row.max_price || row.min_price || 0),
      stock: Number(row.total_stock || 0),
      sold,
      soldText: this.soldText(sold),
      favorites: Number(row.favorite_count || 0),
      shippingFrom: String(detail.shippingFrom || ''),
      freeShipping: Number(row.is_free_shipping || 0) === 1,
      purchaseLimit: Number(detail.purchaseLimit || 0),
      imageUrl: imageUrls[0] || '',
      imageUrls,
      hdImageUrls,
      specs: firstSku?.specs || [],
      skus,
      status: this.statusText(Number(row.status || 0)),
    };
  }

  private mapCoupon(row: CouponRow) {
    const threshold = Number(row.threshold_amount || 0);
    const discount = Number(row.discount_amount || 0);
    const totalCount = Number(row.total_count || 0);
    const receivedCount = Number(row.received_count || 0);
    const scope = this.normalizeCouponLevel(row.coupon_level, row);
    const receiveMode = this.normalizeReceiveMode(row.receive_mode, Number(row.is_once_per_user || 0) === 1);
    return {
      id: row.id,
      code: row.coupon_code,
      shopId: row.shop_id,
      productId: row.product_id,
      productName: row.product_name || '',
      title: row.title,
      scope,
      couponLevel: scope,
      threshold,
      thresholdText: threshold > 0 ? `满${threshold}减` : '无门槛',
      discount,
      stackable: Number(row.is_stackable || 0) === 1,
      oncePerUser: receiveMode === 'once',
      receiveMode,
      totalCount,
      receivedCount,
      remainingCount: totalCount > 0 ? Math.max(totalCount - receivedCount, 0) : 999999,
      unlimitedCount: totalCount === 0,
      endAt: row.end_at ? String(row.end_at).replace('T', ' ').slice(0, 19) : '',
      status: Number(row.status || 0) === 1 ? '启用' : '停用',
    };
  }

  private mapMyCoupon(row: MyCouponRow) {
    return {
      ...this.mapCoupon(row),
      shopName: row.shop_name || '平台优惠券',
      shopAvatarUrl: row.shop_avatar_url || '',
      claimedAt: row.claimed_at ? String(row.claimed_at) : '',
    };
  }

  private normalizeReceiveMode(value: any, oncePerUser = false) {
    const mode = String(value || '').trim();
    if (mode === 'grant_only') return 'grant_only';
    if (mode === 'unlimited') return 'unlimited';
    if (mode === 'once') return 'once';
    return oncePerUser ? 'once' : 'unlimited';
  }

  private normalizeCouponLevel(value: any, row?: Pick<CouponRow, 'shop_id' | 'product_id'>) {
    const level = String(value || '').trim();
    if (level === 'product' || level === 'shop' || level === 'platform') return level;
    return row?.product_id ? 'product' : row?.shop_id ? 'shop' : 'platform';
  }

  private async ensureCouponReceiveModeColumn() {
    const columns = await this.db.query<any>(`SHOW COLUMNS FROM ${this.table('market_coupons')};`);
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('coupon_level')) {
      await this.db.query(
        `ALTER TABLE ${this.table('market_coupons')} ADD COLUMN \`coupon_level\` VARCHAR(24) NOT NULL DEFAULT 'shop' COMMENT '优惠券层级：product商品 shop店铺 platform平台' AFTER \`is_once_per_user\`;`,
      );
      await this.db.query(
        `UPDATE ${this.table('market_coupons')} SET \`coupon_level\` = CASE WHEN \`product_id\` IS NOT NULL THEN 'product' WHEN \`shop_id\` IS NOT NULL THEN 'shop' ELSE 'platform' END;`,
      );
    }
    if (!names.has('receive_mode')) {
      await this.db.query(
        `ALTER TABLE ${this.table('market_coupons')} ADD COLUMN \`receive_mode\` VARCHAR(24) NOT NULL DEFAULT 'unlimited' COMMENT '领取方式：once单用户一次 unlimited不限领取 grant_only仅后台发放' AFTER \`coupon_level\`;`,
      );
      await this.db.query(
        `UPDATE ${this.table('market_coupons')} SET \`receive_mode\` = CASE WHEN \`is_once_per_user\` = 1 THEN 'once' ELSE 'unlimited' END;`,
      );
    }
  }

  private async getAccountFromRequest(req: any) {
    const token = this.authTokenService.extractToken(req);
    if (!token) return '';
    const check = await this.authTokenService.verifyToken(token);
    return check.ok ? String(check.account || '').trim() : '';
  }

  private async ensureOrderCouponColumn() {
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS \`order\` (
        \`account\` VARCHAR(255) NOT NULL,
        \`market_coupons\` JSON NULL,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (\`account\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    const columns = await this.db.query<any>('SHOW COLUMNS FROM `order`;');
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('market_coupons')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `market_coupons` JSON NULL;');
    }
    if (!names.has('created_at')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `created_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP;');
    }
    if (!names.has('updated_at')) {
      await this.db.query('ALTER TABLE `order` ADD COLUMN `updated_at` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP;');
    }
  }

  private async ensureMarketOrderTables() {
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS ${this.table('market_orders')} (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`order_no\` VARCHAR(64) NOT NULL,
        \`user_id\` BIGINT UNSIGNED NOT NULL DEFAULT 0,
        \`user_account\` VARCHAR(255) NULL,
        \`shop_id\` BIGINT UNSIGNED NULL,
        \`receiver_snapshot\` JSON NULL,
        \`product_amount\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`freight_amount\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`discount_amount\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`pay_amount\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`payment_method\` VARCHAR(40) NULL,
        \`remark\` VARCHAR(300) NULL,
        \`coupon_snapshot\` JSON NULL,
        \`refund_status\` TINYINT UNSIGNED NULL,
        \`refund_reason\` VARCHAR(300) NULL,
        \`refund_received_status\` VARCHAR(20) NULL,
        \`refund_applied_at\` DATETIME NULL,
        \`refund_reviewed_at\` DATETIME NULL,
        \`refund_origin_status\` TINYINT UNSIGNED NULL,
        \`status\` TINYINT UNSIGNED NOT NULL DEFAULT 10,
        \`paid_at\` DATETIME NULL,
        \`cancelled_at\` DATETIME NULL,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        \`deleted_at\` DATETIME NULL,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`uk_market_orders_order_no\` (\`order_no\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS ${this.table('market_order_items')} (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`order_id\` BIGINT UNSIGNED NOT NULL,
        \`product_id\` BIGINT UNSIGNED NULL,
        \`sku_id\` BIGINT UNSIGNED NULL,
        \`product_snapshot\` JSON NOT NULL,
        \`quantity\` INT UNSIGNED NOT NULL DEFAULT 1,
        \`unit_price\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`total_amount\` DECIMAL(10, 2) NOT NULL DEFAULT 0.00,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (\`id\`),
        KEY \`idx_market_order_items_order\` (\`order_id\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    const columns = await this.db.query<any>(`SHOW COLUMNS FROM ${this.table('market_orders')};`);
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('user_account')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`user_account\` VARCHAR(255) NULL AFTER \`user_id\`;`);
    }
    if (!names.has('cancelled_at')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`cancelled_at\` DATETIME NULL AFTER \`paid_at\`;`);
    }
    if (!names.has('coupon_snapshot')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`coupon_snapshot\` JSON NULL AFTER \`remark\`;`);
    }
    if (!names.has('shipped_at')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`shipped_at\` DATETIME NULL AFTER \`paid_at\`;`);
    }
    if (!names.has('finished_at')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`finished_at\` DATETIME NULL AFTER \`shipped_at\`;`);
    }
    if (!names.has('refund_status')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_status\` TINYINT UNSIGNED NULL AFTER \`coupon_snapshot\`;`);
    }
    if (!names.has('refund_reason')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_reason\` VARCHAR(300) NULL AFTER \`refund_status\`;`);
    }
    if (!names.has('refund_received_status')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_received_status\` VARCHAR(20) NULL AFTER \`refund_reason\`;`);
    }
    if (!names.has('refund_applied_at')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_applied_at\` DATETIME NULL AFTER \`refund_received_status\`;`);
    }
    if (!names.has('refund_reviewed_at')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_reviewed_at\` DATETIME NULL AFTER \`refund_applied_at\`;`);
    }
    if (!names.has('refund_origin_status')) {
      await this.db.query(`ALTER TABLE ${this.table('market_orders')} ADD COLUMN \`refund_origin_status\` TINYINT UNSIGNED NULL AFTER \`refund_reviewed_at\`;`);
    }
  }

  private async ensureReviewTable() {
    await this.ensureMarketOrderTables();
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS ${this.table('market_product_reviews')} (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`order_id\` BIGINT UNSIGNED NOT NULL,
        \`order_item_id\` BIGINT UNSIGNED NOT NULL,
        \`product_id\` BIGINT UNSIGNED NOT NULL,
        \`shop_id\` BIGINT UNSIGNED NULL,
        \`user_account\` VARCHAR(255) NOT NULL,
        \`user_name\` VARCHAR(120) NULL,
        \`rating\` TINYINT UNSIGNED NOT NULL DEFAULT 5,
        \`content\` TEXT NOT NULL,
        \`reply_content\` TEXT NULL,
        \`reply_shop_name\` VARCHAR(120) NULL,
        \`replied_at\` DATETIME NULL,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        \`deleted_at\` DATETIME NULL,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`uk_market_product_reviews_order_item\` (\`order_item_id\`),
        KEY \`idx_market_product_reviews_product\` (\`product_id\`, \`created_at\`),
        KEY \`idx_market_product_reviews_shop\` (\`shop_id\`, \`created_at\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
  }

  private async ensureServiceTables() {
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS ${this.table('market_service_sessions')} (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`session_no\` VARCHAR(64) NOT NULL,
        \`user_id\` BIGINT UNSIGNED NOT NULL DEFAULT 0,
        \`user_account\` VARCHAR(255) NULL,
        \`shop_id\` BIGINT UNSIGNED NULL,
        \`product_id\` BIGINT UNSIGNED NULL,
        \`status\` TINYINT UNSIGNED NOT NULL DEFAULT 1,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        \`updated_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
        PRIMARY KEY (\`id\`),
        UNIQUE KEY \`uk_market_service_sessions_session_no\` (\`session_no\`),
        KEY \`idx_market_service_sessions_user_status\` (\`user_account\`, \`status\`, \`updated_at\`),
        KEY \`idx_market_service_sessions_shop_status\` (\`shop_id\`, \`status\`, \`updated_at\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    await this.db.query(
      `CREATE TABLE IF NOT EXISTS ${this.table('market_service_messages')} (
        \`id\` BIGINT UNSIGNED NOT NULL AUTO_INCREMENT,
        \`session_id\` BIGINT UNSIGNED NOT NULL,
        \`sender_type\` TINYINT UNSIGNED NOT NULL,
        \`sender_id\` BIGINT UNSIGNED NULL,
        \`message_type\` TINYINT UNSIGNED NOT NULL DEFAULT 1,
        \`content\` TEXT NULL,
        \`payload\` JSON NULL,
        \`is_read\` TINYINT UNSIGNED NOT NULL DEFAULT 0,
        \`created_at\` DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP,
        PRIMARY KEY (\`id\`),
        KEY \`idx_market_service_messages_session_time\` (\`session_id\`, \`created_at\`)
      ) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci;`,
    );
    const columns = await this.db.query<any>(`SHOW COLUMNS FROM ${this.table('market_service_sessions')};`);
    const names = new Set(columns.map((item) => item.Field));
    if (!names.has('user_account')) {
      await this.db.query(`ALTER TABLE ${this.table('market_service_sessions')} ADD COLUMN \`user_account\` VARCHAR(255) NULL AFTER \`user_id\`;`);
    }
    const userId = columns.find((item) => item.Field === 'user_id');
    if (userId && !String(userId.Default ?? '').length && String(userId.Null).toUpperCase() === 'NO') {
      await this.db.query(`ALTER TABLE ${this.table('market_service_sessions')} MODIFY COLUMN \`user_id\` BIGINT UNSIGNED NOT NULL DEFAULT 0;`);
    }
  }

  private parseJsonValue(value: any) {
    if (!value) return null;
    if (typeof value === 'object') return value;
    try {
      return JSON.parse(value);
    } catch {
      return null;
    }
  }

  private statusTextFromOrder(status: number) {
    if (status === 20) return '待发货';
    if (status === 30) return '待收货/使用';
    if (status === 40) return '评价';
    if (status === 50) return '已取消';
    if (status === 60) return '售后';
    return '待付款';
  }

  private refundStatusText(status: number | null | undefined) {
    if (Number(status || 0) === 1) return '商家审核中';
    if (Number(status || 0) === 2) return '商家同意退款';
    if (Number(status || 0) === 3) return '商家拒绝退款';
    return '';
  }

  private formatTime(value: Date | string | null | undefined) {
    if (!value) return '';
    const date = value instanceof Date ? value : new Date(value);
    if (Number.isNaN(date.getTime())) return String(value || '');
    const pad = (input: number) => String(input).padStart(2, '0');
    return `${date.getFullYear()}-${pad(date.getMonth() + 1)}-${pad(date.getDate())}`;
  }

  private maskAccount(account: string) {
    const value = String(account || '').trim();
    if (value.length <= 4) return value || '买家';
    if (/^\d{8,}$/.test(value)) return `${value.slice(0, 3)}****${value.slice(-4)}`;
    return `${value.slice(0, 2)}***${value.slice(-1)}`;
  }

  private mapReview(row: MarketReviewRow) {
    return {
      id: row.id,
      productId: row.product_id,
      orderId: row.order_id,
      orderItemId: row.order_item_id,
      userName: row.user_name || this.maskAccount(row.user_account),
      userAvatarUrl: row.user_avatar || '',
      rating: Number(row.rating || 5),
      content: row.content,
      createdAt: this.formatTime(row.created_at),
      merchantReply: row.reply_content
        ? {
            content: row.reply_content,
            shopName: row.reply_shop_name || '商家',
            repliedAt: this.formatTime(row.replied_at),
            isMerchantReply: true,
          }
        : null,
    };
  }

  private mapServiceMessage(row: MarketServiceMessageRow) {
    return {
      id: row.id,
      sessionId: row.session_id,
      sender: Number(row.sender_type) === 2 ? 'merchant' : Number(row.sender_type) === 3 ? 'ai' : 'user',
      messageType: Number(row.message_type || 1) === 2 ? 'product' : 'text',
      content: row.content || '',
      payload: this.parseJsonValue(row.payload),
      createdAt: new Date(row.created_at).getTime() || Date.now(),
    };
  }

  private normalizeServiceProductPayload(value: any): ServiceProductPayload | null {
    if (!value || typeof value !== 'object') return null;
    const raw = value as Record<string, unknown>;
    const payload: ServiceProductPayload = {
      productId: Number(raw.productId || 0) || undefined,
      name: String(raw.name || '').trim() || undefined,
      imageUrl: String(raw.imageUrl || '').trim() || undefined,
      price: Number(raw.price || 0) || undefined,
      specText: String(raw.specText || '').trim() || undefined,
      orderNo: String(raw.orderNo || '').trim() || undefined,
      orderStatus: String(raw.orderStatus || '').trim() || undefined,
      orderQuantity: String(raw.orderQuantity || '').trim() || undefined,
      orderTotal: String(raw.orderTotal || '').trim() || undefined,
    };
    const hasCore = Boolean(payload.productId || payload.name || payload.imageUrl || payload.price);
    if (!hasCore) return null;
    return payload;
  }

  private mapServiceSession(row: MarketServiceSessionRow, messages: MarketServiceMessageRow[] = []) {
    return {
      id: row.id,
      sessionNo: row.session_no,
      account: row.user_account || '',
      shopId: row.shop_id,
      shop: row.shop_name || '店铺客服',
      shopAvatarUrl: row.shop_avatar_url || '',
      shopServiceLevel: row.shop_service_level || '金牌客服',
      shopSales: Number(row.shop_sales_count || 0),
      productId: row.product_id,
      product: row.product_name || '',
      productImageUrl: row.product_image_url || '',
      productPrice: Number(row.product_price || 0),
      status: Number(row.status || 1) === 1 ? '进行中' : '已关闭',
      unreadCount: Number(row.unread_count || 0),
      updatedAt: new Date(row.updated_at).getTime() || Date.now(),
      orderTags: [] as ServiceOrderTag[],
      messages: messages.map((item) => this.mapServiceMessage(item)),
    };
  }

  private mapServiceOrderTag(row: ServiceOrderTagRow): ServiceOrderTag {
    return {
      orderId: Number(row.id || 0),
      orderNo: String(row.order_no || ''),
      status: this.statusTextFromOrder(Number(row.status || 10)),
      createdAt: new Date(row.created_at).getTime() || Date.now(),
      total: Number(row.pay_amount || 0),
    };
  }

  private async queryServiceOrderTags(account: string, shopId: number) {
    if (!account || !shopId) return [] as ServiceOrderTag[];
    const rows = await this.db.query<ServiceOrderTagRow>(
      `SELECT id, order_no, status, created_at, pay_amount
       FROM ${this.table('market_orders')}
       WHERE user_account = ? AND shop_id = ? AND deleted_at IS NULL
       ORDER BY created_at DESC, id DESC
       LIMIT 20;`,
      [account, shopId],
    );
    return rows.map((row) => this.mapServiceOrderTag(row));
  }

  private async attachServiceOrderTags(
    sessions: Array<ReturnType<MarketService['mapServiceSession']>>,
  ) {
    if (!sessions.length) return sessions;
    const keyMap = new Map<string, { account: string; shopId: number }>();
    sessions.forEach((session) => {
      const account = String(session.account || '').trim();
      const shopId = Number(session.shopId || 0);
      if (!account || !shopId) return;
      const key = `${account}__${shopId}`;
      if (!keyMap.has(key)) keyMap.set(key, { account, shopId });
    });
    const tagMap = new Map<string, ServiceOrderTag[]>();
    for (const [key, value] of keyMap) {
      tagMap.set(key, await this.queryServiceOrderTags(value.account, value.shopId));
    }
    return sessions.map((session) => {
      const account = String(session.account || '').trim();
      const shopId = Number(session.shopId || 0);
      const key = `${account}__${shopId}`;
      return {
        ...session,
        orderTags: tagMap.get(key) || [],
      };
    });
  }

  private async consolidateServiceSessionByAccountShop(account: string, shopId: number, preferredProductId?: number | null) {
    if (!account || !shopId) return 0;
    const rows = await this.db.query<Array<{ id: number; product_id: number | null; updated_at: Date | string }>[number]>(
      `SELECT id, product_id, updated_at
       FROM ${this.table('market_service_sessions')}
       WHERE user_account = ? AND shop_id = ? AND status = 1
       ORDER BY updated_at DESC, id DESC;`,
      [account, shopId],
    );
    if (!rows.length) return 0;
    const master = rows[0];
    if (rows.length > 1) {
      const slaveIds = rows.slice(1).map((item) => Number(item.id || 0)).filter((id) => id > 0);
      if (slaveIds.length) {
        await this.db.query(
          `UPDATE ${this.table('market_service_messages')}
           SET session_id = ?
           WHERE session_id IN (${slaveIds.map(() => '?').join(',')});`,
          [master.id, ...slaveIds],
        );
        await this.db.query(
          `UPDATE ${this.table('market_service_sessions')}
           SET status = 0, updated_at = CURRENT_TIMESTAMP
           WHERE id IN (${slaveIds.map(() => '?').join(',')});`,
          slaveIds,
        );
      }
    }
    const normalizedProductId = Number(preferredProductId || 0);
    const hasProduct = Number(master.product_id || 0) > 0;
    if (!hasProduct && normalizedProductId > 0) {
      await this.db.query(
        `UPDATE ${this.table('market_service_sessions')}
         SET product_id = ?, updated_at = CURRENT_TIMESTAMP
         WHERE id = ?;`,
        [normalizedProductId, master.id],
      );
    }
    return Number(master.id || 0);
  }

  private async ensureSingleServiceSession(account: string, shopId: number, preferredProductId?: number | null) {
    const consolidatedId = await this.consolidateServiceSessionByAccountShop(account, shopId, preferredProductId);
    if (consolidatedId) return consolidatedId;
    const sessionNo = `CS${Date.now()}${Math.floor(Math.random() * 1000).toString().padStart(3, '0')}`;
    const result: any = await this.db.query(
      `INSERT INTO ${this.table('market_service_sessions')} (session_no, user_id, user_account, shop_id, product_id, status)
       VALUES (?, 0, ?, ?, ?, 1);`,
      [sessionNo, account, shopId, Number(preferredProductId || 0) || null],
    );
    return Number(result.insertId || 0);
  }

  private async consolidateAllUserServiceSessions(account: string) {
    if (!account) return;
    const rows = await this.db.query<Array<{ shop_id: number | null }>[number]>(
      `SELECT shop_id
       FROM ${this.table('market_service_sessions')}
       WHERE user_account = ? AND status = 1 AND shop_id IS NOT NULL
       GROUP BY shop_id
       HAVING COUNT(1) > 1;`,
      [account],
    );
    for (const row of rows) {
      const shopId = Number(row.shop_id || 0);
      if (!shopId) continue;
      await this.consolidateServiceSessionByAccountShop(account, shopId);
    }
  }

  private normalizeOrderItems(rawItems: any[]) {
    return (Array.isArray(rawItems) ? rawItems : [])
      .map((item) => {
        const quantity = Math.max(1, Number(item?.quantity || 1));
        const price = Math.max(0, Number(item?.price || item?.unitPrice || 0));
        return {
          key: String(item?.key || `${item?.productId || ''}:${item?.skuId ?? 'default'}`),
          productId: String(item?.productId || ''),
          skuId: item?.skuId == null ? null : Number(item.skuId),
          shopId: item?.shopId == null ? null : Number(item.shopId),
          shop: String(item?.shop || '默认店铺'),
          shopAvatarUrl: String(item?.shopAvatarUrl || ''),
          name: String(item?.name || '商品'),
          imageUrl: String(item?.imageUrl || ''),
          specText: String(item?.specText || ''),
          price,
          originPrice: Math.max(0, Number(item?.originPrice || price)),
          quantity,
          soldText: String(item?.soldText || ''),
          addedAt: Number(item?.addedAt || Date.now()),
          totalAmount: Number((price * quantity).toFixed(2)),
        };
      })
      .filter((item) => item.productId && item.totalAmount > 0);
  }

  private normalizeReceiverSnapshot(address: any) {
    if (!address || typeof address !== 'object') return null;
    const regionText = String(address.region || '').trim();
    const regionParts = regionText ? regionText.split(/\s+/).filter(Boolean) : [];
    const province = String(address.province || regionParts[0] || '').trim();
    const city = String(address.city || regionParts[1] || '').trim();
    const district = String(address.district || regionParts[2] || '').trim();
    const detailAddress = String(address.detailAddress || address.detail_address || address.detail || address.address || '').trim();
    const receiver = String(address.receiver || address.consignee || address.name || '').trim();
    const mobile = String(address.mobile || address.phone || '').trim();
    const normalized = {
      ...address,
      region: regionText || [province, city, district].filter(Boolean).join(' '),
      detail: String(address.detail || detailAddress).trim(),
      name: String(address.name || receiver).trim(),
      phone: String(address.phone || mobile).trim(),
      province,
      city,
      district,
      detailAddress,
      receiver,
      mobile,
    };
    const hasAddress = Boolean(
      normalized.region ||
        normalized.detail ||
        normalized.detailAddress ||
        normalized.province ||
        normalized.city ||
        normalized.district,
    );
    return hasAddress ? normalized : null;
  }

  private mapOrder(row: MarketOrderRow, itemRows: MarketOrderItemRow[], reviewedOrderIds = new Set<number>()) {
    const items = itemRows.map((item) => {
      const snapshot: any = this.parseJsonValue(item.product_snapshot) || {};
      return {
        key: snapshot.key || `${item.product_id || snapshot.productId || ''}:${item.sku_id ?? snapshot.skuId ?? 'default'}`,
        productId: String(item.product_id || snapshot.productId || ''),
        skuId: item.sku_id ?? snapshot.skuId ?? null,
        shopId: row.shop_id ?? snapshot.shopId ?? null,
        shop: row.shop_name || snapshot.shop || '默认店铺',
        shopAvatarUrl: row.shop_avatar_url || snapshot.shopAvatarUrl || '',
        name: snapshot.name || '商品',
        imageUrl: snapshot.imageUrl || '',
        specText: snapshot.specText || '',
        price: Number(item.unit_price || snapshot.price || 0),
        originPrice: Number(snapshot.originPrice || item.unit_price || 0),
        quantity: Number(item.quantity || 1),
        soldText: snapshot.soldText || '',
        addedAt: Number(snapshot.addedAt || Date.now()),
      };
    });
    const createdAt = row.created_at ? new Date(row.created_at).getTime() : Date.now();
    return {
      id: String(row.id),
      orderNo: row.order_no,
      status: this.statusTextFromOrder(Number(row.status || 10)),
      shop: row.shop_name || items[0]?.shop || '默认店铺',
      shopId: row.shop_id,
      shopAvatarUrl: row.shop_avatar_url || items[0]?.shopAvatarUrl || '',
      items,
      subtotal: Number(row.product_amount || 0),
      discount: Number(row.discount_amount || 0),
      total: Number(row.pay_amount || 0),
      address: this.parseJsonValue(row.receiver_snapshot),
      refundStatus: this.refundStatusText(row.refund_status),
      refundReason: row.refund_reason || '',
      refundReceivedStatus: row.refund_received_status || '',
      refundAppliedAt: row.refund_applied_at ? new Date(row.refund_applied_at).getTime() : 0,
      createdAt,
      payDeadline: createdAt + ORDER_PAY_TIMEOUT_MS,
      reviewed: reviewedOrderIds.has(Number(row.id)),
    };
  }

  private async reserveOrderStock(items: ReturnType<MarketService['normalizeOrderItems']>) {
    const reserved: Array<{ skuId: number | null; productId: number; quantity: number }> = [];
    try {
      for (const item of items) {
        const record = { skuId: null as number | null, productId: 0, quantity: item.quantity };
        if (item.skuId) {
          const result: any = await this.db.query(
            `UPDATE ${this.table('skus')} SET stock = stock - ? WHERE id = ? AND stock >= ?;`,
            [item.quantity, item.skuId, item.quantity],
          );
          if (!result.affectedRows) {
            await this.rollbackReservedStock(reserved);
            return false;
          }
          record.skuId = item.skuId;
        }
        reserved.push(record);
        const productId = Number(item.productId || 0);
        if (productId > 0) {
          await this.db.query(
            `UPDATE ${this.table('products')} SET total_stock = GREATEST(total_stock - ?, 0) WHERE id = ?;`,
            [item.quantity, productId],
          );
          record.productId = productId;
        }
      }
    } catch (error) {
      await this.rollbackReservedStock(reserved);
      throw error;
    }
    return true;
  }

  private async rollbackReservedStock(items: Array<{ skuId: number | null; productId: number; quantity: number }>) {
    for (const item of items.reverse()) {
      if (item.skuId) {
        await this.db.query(`UPDATE ${this.table('skus')} SET stock = stock + ? WHERE id = ?;`, [item.quantity, item.skuId]);
      }
      if (item.productId > 0) {
        await this.db.query(`UPDATE ${this.table('products')} SET total_stock = total_stock + ? WHERE id = ?;`, [item.quantity, item.productId]);
      }
    }
  }

  private async restoreOrderStock(orderId: number) {
    const items = await this.db.query<MarketOrderItemRow>(
      `SELECT * FROM ${this.table('market_order_items')} WHERE order_id = ?;`,
      [orderId],
    );
    for (const item of items) {
      const quantity = Number(item.quantity || 0);
      if (quantity <= 0) continue;
      if (item.sku_id) {
        await this.db.query(`UPDATE ${this.table('skus')} SET stock = stock + ? WHERE id = ?;`, [quantity, item.sku_id]);
      }
      if (item.product_id) {
        await this.db.query(`UPDATE ${this.table('products')} SET total_stock = total_stock + ? WHERE id = ?;`, [quantity, item.product_id]);
      }
    }
  }

  private normalizeOrderCouponIds(body: any) {
    const values = [
      ...(Array.isArray(body?.couponIds) ? body.couponIds : []),
      ...(Array.isArray(body?.coupons) ? body.coupons.map((item: any) => item?.id || item?.couponId) : []),
      ...Object.values(body?.couponSelection || {}),
      ...Object.values(body?.selectedCouponIds || {}),
    ];
    return Array.from(
      new Set(
        values
          .map((value) => Number(value || 0))
          .filter((value) => Number.isFinite(value) && value > 0),
      ),
    );
  }

  private couponIdsFromSnapshot(value: any) {
    const snapshot: any = this.parseJsonValue(value) || {};
    return Array.isArray(snapshot.couponIds)
      ? snapshot.couponIds.map((id: any) => Number(id)).filter((id: number) => Number.isFinite(id) && id > 0)
      : [];
  }

  private async validateUsableCoupons(account: string, couponIds: number[]) {
    if (!couponIds.length) return true;
    const claimed = await this.readClaimedCoupons(account);
    const usable = new Set(claimed.filter((item) => !item.usedAt).map((item) => item.couponId));
    return couponIds.every((couponId) => usable.has(couponId));
  }

  private async markCouponsUsed(account: string, couponIds: number[], orderIds: number[]) {
    if (!couponIds.length || !orderIds.length) return;
    const usedAt = Date.now();
    const couponSet = new Set(couponIds);
    const claimed = await this.readClaimedCoupons(account);
    const next = claimed.map((item) => {
      if (!couponSet.has(item.couponId)) return item;
      const usedOrderIds = Array.from(new Set([...(item.usedOrderIds || []), ...orderIds]));
      return { ...item, usedAt: item.usedAt || usedAt, usedOrderIds };
    });
    await this.writeClaimedCoupons(account, next);
  }

  private returnCouponsToWallet(items: CouponWalletItem[], couponIds: number[], orderId: number) {
    if (!couponIds.length) return items;
    const couponSet = new Set(couponIds);
    return items.map((item) => {
      if (!couponSet.has(item.couponId)) return item;
      const usedOrderIds = (item.usedOrderIds || []).filter((id) => id !== orderId);
      if (usedOrderIds.length) return { ...item, usedOrderIds };
      const { usedAt, usedOrderIds: _usedOrderIds, ...unused } = item;
      return unused;
    });
  }

  async cancelUnpaidOrder(orderId: number) {
    await this.ensureMarketOrderTables();
    await this.ensureOrderCouponColumn();
    return this.db.transaction(async (query) => {
      const orderRows = await query<MarketOrderRow[]>(
        `SELECT id, user_account, coupon_snapshot, status
         FROM ${this.table('market_orders')}
         WHERE id = ? AND deleted_at IS NULL
         LIMIT 1
         FOR UPDATE;`,
        [orderId],
      );
      const order = orderRows[0];
      if (!order || Number(order.status) !== 10) return false;

      const items = await query<MarketOrderItemRow[]>(
        `SELECT * FROM ${this.table('market_order_items')} WHERE order_id = ? FOR UPDATE;`,
        [orderId],
      );
      for (const item of items) {
        const quantity = Number(item.quantity || 0);
        if (quantity <= 0) continue;
        if (item.sku_id) {
          await query(`UPDATE ${this.table('skus')} SET stock = stock + ? WHERE id = ?;`, [quantity, item.sku_id]);
        }
        if (item.product_id) {
          await query(`UPDATE ${this.table('products')} SET total_stock = total_stock + ? WHERE id = ?;`, [quantity, item.product_id]);
        }
      }

      const account = String(order.user_account || '').trim();
      const couponIds = this.couponIdsFromSnapshot(order.coupon_snapshot);
      if (account && couponIds.length) {
        const walletRows = await query<any[]>('SELECT `market_coupons` FROM `order` WHERE `account` = ? LIMIT 1 FOR UPDATE;', [account]);
        if (walletRows.length) {
          const wallet = this.returnCouponsToWallet(this.normalizeClaimedCoupons(walletRows[0]?.market_coupons), couponIds, orderId);
          await query('UPDATE `order` SET `market_coupons` = ?, `updated_at` = CURRENT_TIMESTAMP WHERE `account` = ?;', [JSON.stringify(wallet), account]);
        }
      }

      const result = await query<any>(
        `UPDATE ${this.table('market_orders')}
         SET status = 50, cancelled_at = NOW()
         WHERE id = ? AND status = 10 AND deleted_at IS NULL;`,
        [orderId],
      );
      return Boolean(result.affectedRows);
    });
  }

  private async expireOverdueUnpaidOrders(account?: string) {
    await this.ensureMarketOrderTables();
    const params: any[] = [];
    const accountSql = account ? 'AND user_account = ?' : '';
    if (account) params.push(account);
    const rows = await this.db.query<{ id: number }>(
      `SELECT id FROM ${this.table('market_orders')}
       WHERE status = 10 AND deleted_at IS NULL ${accountSql}
         AND created_at <= DATE_SUB(NOW(), INTERVAL 30 MINUTE)
       LIMIT 50;`,
      params,
    );
    for (const row of rows) {
      await this.cancelUnpaidOrder(Number(row.id));
    }
  }

  private async scheduleUnpaidOrderCancel(orderId: number) {
    await this.orderQueue.add(
      'cancel-unpaid-order',
      { orderId },
      {
        delay: ORDER_PAY_TIMEOUT_MS,
        jobId: `cancel-unpaid-order:${orderId}`,
        removeOnComplete: true,
        removeOnFail: 100,
      },
    );
  }

  private normalizeClaimedCoupons(value: any) {
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
      .map((item) => {
        const usedOrderIds = Array.isArray(item?.usedOrderIds)
          ? item.usedOrderIds.map((id: any) => Number(id)).filter((id: number) => Number.isFinite(id) && id > 0)
          : [];
        return {
          couponId: Number(item?.couponId || item?.id || 0),
          claimedAt: Number(item?.claimedAt || Date.now()),
          usedAt: Number(item?.usedAt || 0) || undefined,
          usedOrderIds: usedOrderIds.length ? usedOrderIds : undefined,
        };
      })
      .filter((item) => Number.isFinite(item.couponId) && item.couponId > 0);
  }

  private async readClaimedCoupons(account: string) {
    await this.ensureOrderCouponColumn();
    await this.db.query('INSERT IGNORE INTO `order` (`account`, `market_coupons`) VALUES (?, JSON_ARRAY());', [account]);
    const rows = await this.db.query<any>('SELECT `market_coupons` FROM `order` WHERE `account` = ? LIMIT 1;', [account]);
    return this.normalizeClaimedCoupons(rows[0]?.market_coupons);
  }

  private async writeClaimedCoupons(account: string, items: CouponWalletItem[]) {
    await this.ensureOrderCouponColumn();
    await this.db.query(
      `INSERT INTO \`order\` (\`account\`, \`market_coupons\`) VALUES (?, ?)
       ON DUPLICATE KEY UPDATE \`market_coupons\` = VALUES(\`market_coupons\`), \`updated_at\` = CURRENT_TIMESTAMP;`,
      [account, JSON.stringify(items)],
    );
  }

  async home() {
    const categoryRows = await this.db.query<CategoryRow>(
      `SELECT c.*,
              GROUP_CONCAT(f.title ORDER BY f.row_index ASC, f.sort_order ASC SEPARATOR '||') AS feature_titles
       FROM ${this.table('market_categories')} c
       LEFT JOIN ${this.table('market_category_features')} f ON f.category_id = c.id AND f.status = 1
       WHERE c.deleted_at IS NULL AND c.status = 1 AND c.level = 1
       GROUP BY c.id
       ORDER BY c.sort_order ASC, c.id ASC;`,
    );
    const childRows = await this.db.query<CategoryChildRow>(
      `SELECT id, parent_id, category_key, name, icon_url, sort_order
       FROM ${this.table('market_categories')}
       WHERE deleted_at IS NULL AND status = 1 AND level = 2
       ORDER BY parent_id ASC, sort_order ASC, id ASC;`,
    );
    const childrenByParent = childRows.reduce<Record<number, CategoryChildRow[]>>((acc, row) => {
      if (!acc[row.parent_id]) acc[row.parent_id] = [];
      acc[row.parent_id].push(row);
      return acc;
    }, {});
    const products = await this.products({ limit: 20 });
    return {
      status: 200,
      message: 'Fetch success.',
      result: {
        categories: categoryRows.map((row) => ({
          id: row.id,
          key: row.category_key,
          title: row.name,
          iconUrl: row.icon_url || '',
          features: row.feature_titles ? row.feature_titles.split('||').filter(Boolean) : [],
          children: (childrenByParent[row.id] || []).map((child) => ({
            id: child.id,
            key: child.category_key,
            title: child.name,
            iconUrl: child.icon_url || '',
          })),
        })),
        products: products.result,
      },
    };
  }

  async products(query: any) {
    const categoryId = Number(query?.categoryId || 0);
    const keyword = String(query?.keyword || '').trim();
    const shopId = Number(query?.shopId || 0);
    const limit = Math.max(1, Math.min(Number(query?.limit || 20), 50));
    const offset = Math.max(0, Number(query?.offset || 0));
    const where = ['p.deleted_at IS NULL', 'p.status = 1'];
    const params: any[] = [];

    if (categoryId > 0) {
      where.push(`(p.category_id = ? OR p.category_id IN (
        SELECT child.id FROM ${this.table('market_categories')} child
        LEFT JOIN ${this.table('market_categories')} parent ON parent.id = child.parent_id
        WHERE child.deleted_at IS NULL AND (child.parent_id = ? OR parent.parent_id = ?)
      ))`);
      params.push(categoryId, categoryId, categoryId);
    }
    if (shopId > 0) {
      where.push('p.shop_id = ?');
      params.push(shopId);
    }
    if (keyword) {
      where.push('(p.name LIKE ? OR c.name LIKE ? OR u.shop_name LIKE ?)');
      params.push(`%${keyword}%`, `%${keyword}%`, `%${keyword}%`);
    }

    const rows = await this.db.query<ProductRow>(
      `${this.productSelectSql()}
       WHERE ${where.join(' AND ')}
       ORDER BY p.sort_order ASC, p.id DESC
       LIMIT ? OFFSET ?;`,
      [...params, limit, offset],
    );

    const result: ReturnType<MarketService['mapProduct']>[] = [];
    for (const row of rows) {
      result.push(this.mapProduct(row, await this.getSkuRows(row.id)));
    }
    return { status: 200, message: 'Fetch success.', result };
  }

  async categoryChildren(idText: string) {
    const id = Number(idText);
    if (!Number.isFinite(id) || id <= 0) return { status: 400, message: 'Invalid category id.', result: [] };
    const rows = await this.db.query<CategoryChildRow>(
      `SELECT id, parent_id, category_key, name, icon_url, sort_order
       FROM ${this.table('market_categories')}
       WHERE deleted_at IS NULL AND status = 1 AND parent_id = ?
       ORDER BY sort_order ASC, id ASC;`,
      [id],
    );
    return {
      status: 200,
      message: 'Fetch success.',
      result: rows.map((row) => ({
        id: row.id,
        key: row.category_key,
        title: row.name,
        iconUrl: row.icon_url || '',
      })),
    };
  }

  async product(idText: string) {
    const id = Number(idText);
    if (!Number.isFinite(id) || id <= 0) return { status: 400, message: 'Invalid product id.', result: null };
    const rows = await this.db.query<ProductRow>(
      `${this.productSelectSql()}
       WHERE p.id = ? AND p.deleted_at IS NULL AND p.status = 1
       LIMIT 1;`,
      [id],
    );
    if (!rows.length) return { status: 404, message: 'Product not found.', result: null };
    return { status: 200, message: 'Fetch success.', result: this.mapProduct(rows[0], await this.getSkuRows(id)) };
  }

  async productReviews(idText: string) {
    await this.ensureReviewTable();
    const productId = Number(idText);
    if (!Number.isFinite(productId) || productId <= 0) return { status: 400, message: 'Invalid product id.', result: [] };

    const rows = await this.db.query<MarketReviewRow>(
      `SELECT r.*, l.avatar AS user_avatar
       FROM ${this.table('market_product_reviews')} r
       LEFT JOIN \`login\` l ON l.account = r.user_account
       WHERE r.product_id = ? AND r.deleted_at IS NULL
       ORDER BY r.created_at DESC, r.id DESC
       LIMIT 50;`,
      [productId],
    );
    return { status: 200, message: 'Fetch success.', result: rows.map((row) => this.mapReview(row)) };
  }

  async coupons(query: any) {
    await this.ensureCouponReceiveModeColumn();
    const productId = Number(query?.productId || 0);
    const shopId = Number(query?.shopId || 0);
    const where = ['c.deleted_at IS NULL', 'c.status = 1', "(c.receive_mode IS NULL OR c.receive_mode <> 'grant_only')", '(c.end_at IS NULL OR c.end_at >= NOW())', '(c.total_count = 0 OR c.received_count < c.total_count)'];
    const params: any[] = [];

    if (productId > 0) {
      where.push('(c.product_id IS NULL OR c.product_id = ?)');
      params.push(productId);
    }
    if (shopId > 0) {
      where.push('(c.shop_id = ? OR (c.shop_id IS NULL AND c.product_id IS NULL))');
      params.push(shopId);
    }
    if (!productId && !shopId) return { status: 200, message: 'Fetch success.', result: [] };

    const rows = await this.db.query<CouponRow>(
      `SELECT c.*, p.name AS product_name
       FROM ${this.table('market_coupons')} c
       LEFT JOIN ${this.table('products')} p ON p.id = c.product_id
       WHERE ${where.join(' AND ')}
       ORDER BY c.product_id IS NULL ASC, c.discount_amount DESC, c.id DESC
       LIMIT 20;`,
      params,
    );
    return { status: 200, message: 'Fetch success.', result: rows.map((row) => this.mapCoupon(row)) };
  }

  async myCoupons(req: any) {
    await this.ensureCouponReceiveModeColumn();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: [] };

    const claimed = (await this.readClaimedCoupons(account)).filter((item) => !item.usedAt);
    const claimedById = new Map(claimed.map((item) => [item.couponId, item.claimedAt]));
    const ids = Array.from(claimedById.keys());
    if (!ids.length) return { status: 200, message: 'Fetch success.', result: [] };

    const rows = await this.db.query<MyCouponRow>(
      `SELECT c.*, p.name AS product_name,
              COALESCE(u.shop_name, u.nickname, u.username, s.username) AS shop_name,
              u.shop_avatar_url
       FROM ${this.table('market_coupons')} c
       LEFT JOIN ${this.table('products')} p ON p.id = c.product_id
       LEFT JOIN ${this.table('market_shops')} s ON s.id = c.shop_id AND s.deleted_at IS NULL
       LEFT JOIN ${this.table('admin_users')} u ON u.username = s.username
       WHERE c.id IN (${ids.map(() => '?').join(',')}) AND c.deleted_at IS NULL
       ORDER BY c.shop_id ASC, c.end_at IS NULL ASC, c.end_at ASC, c.id DESC;`,
      ids,
    );

    const result = rows.map((row) => this.mapMyCoupon({ ...row, claimed_at: claimedById.get(Number(row.id)) || '' }));
    return { status: 200, message: 'Fetch success.', result };
  }

  async receiveCoupon(req: any, body: any) {
    await this.ensureCouponReceiveModeColumn();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };

    const couponId = Number(body?.couponId || body?.id || 0);
    if (!Number.isFinite(couponId) || couponId <= 0) return { status: 400, message: '优惠券ID不能为空', result: null };

    const rows = await this.db.query<CouponRow>(
      `SELECT c.*, p.name AS product_name
       FROM ${this.table('market_coupons')} c
       LEFT JOIN ${this.table('products')} p ON p.id = c.product_id
       WHERE c.id = ? AND c.deleted_at IS NULL AND c.status = 1 AND (c.receive_mode IS NULL OR c.receive_mode <> 'grant_only') AND (c.end_at IS NULL OR c.end_at >= NOW())
       LIMIT 1;`,
      [couponId],
    );
    const coupon = rows[0];
    if (!coupon) return { status: 404, message: '优惠券不存在或已失效', result: null };

    const claimed = await this.readClaimedCoupons(account);
    if (claimed.some((item) => item.couponId === couponId)) {
      return { status: 200, message: '已领取', result: this.mapCoupon(coupon) };
    }

    const totalCount = Number(coupon.total_count || 0);
    const receivedCount = Number(coupon.received_count || 0);
    if (totalCount > 0 && receivedCount >= totalCount) return { status: 400, message: '优惠券已领完', result: null };

    const updateResult: any = await this.db.query(
      `UPDATE ${this.table('market_coupons')}
       SET received_count = received_count + 1
       WHERE id = ? AND deleted_at IS NULL AND status = 1 AND (receive_mode IS NULL OR receive_mode <> 'grant_only') AND (end_at IS NULL OR end_at >= NOW()) AND (total_count = 0 OR received_count < total_count);`,
      [couponId],
    );
    if (!updateResult.affectedRows) return { status: 400, message: '优惠券已领完', result: null };

    await this.writeClaimedCoupons(account, [{ couponId, claimedAt: Date.now() }, ...claimed]);
    return { status: 200, message: '领取成功', result: this.mapCoupon({ ...coupon, received_count: receivedCount + 1 }) };
  }

  async serviceSession(req: any, query: any) {
    await this.ensureServiceTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const productId = Number(query?.productId || 0);
    const shopId = Number(query?.shopId || 0);
    if (!productId && !shopId) return { status: 400, message: '商品或店铺不能为空', result: null };

    let targetShopId = shopId;
    if (!targetShopId && productId) {
      const productRows = await this.db.query<{ shop_id: number | null }>(
        `SELECT shop_id FROM ${this.table('products')} WHERE id = ? AND deleted_at IS NULL LIMIT 1;`,
        [productId],
      );
      targetShopId = Number(productRows[0]?.shop_id || 0);
    }
    if (!targetShopId) return { status: 400, message: '店铺不存在', result: null };

    const sessionId = await this.ensureSingleServiceSession(account, targetShopId, productId || null);

    await this.db.query(
      `UPDATE ${this.table('market_service_messages')}
       SET is_read = 1
       WHERE session_id = ? AND sender_type IN (2, 3) AND is_read = 0;`,
      [sessionId],
    );

    const sessionRows = await this.queryServiceSessions('s.id = ?', [sessionId]);
    const messageRows = await this.db.query<MarketServiceMessageRow>(
      `SELECT * FROM ${this.table('market_service_messages')} WHERE session_id = ? ORDER BY created_at ASC, id ASC;`,
      [sessionId],
    );
    const mapped = this.mapServiceSession(sessionRows[0], messageRows);
    const withTags = await this.attachServiceOrderTags([mapped]);
    return { status: 200, message: 'Fetch success.', result: withTags[0] || mapped };
  }

  async serviceSessions(req: any) {
    await this.ensureServiceTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: [] };
    await this.consolidateAllUserServiceSessions(account);

    const rows = await this.db.query<MarketServiceSessionRow>(
      `SELECT s.*,
              COALESCE(u.shop_name, u.nickname, u.username, ms.username) AS shop_name,
              u.shop_avatar_url,
              ms.service_level AS shop_service_level,
              ms.sales_count AS shop_sales_count,
              p.name AS product_name,
              p.main_image_url AS product_image_url,
              p.min_price AS product_price,
              (
                SELECT COUNT(1)
                FROM ${this.table('market_service_messages')} um
                WHERE um.session_id = s.id
                  AND um.sender_type IN (2, 3)
                  AND um.is_read = 0
              ) AS unread_count
       FROM ${this.table('market_service_sessions')} s
       LEFT JOIN ${this.table('market_shops')} ms ON ms.id = s.shop_id AND ms.deleted_at IS NULL
       LEFT JOIN ${this.table('admin_users')} u ON u.username = ms.username
       LEFT JOIN ${this.table('products')} p ON p.id = s.product_id
       WHERE s.user_account = ?
         AND s.status = 1
         AND EXISTS (
           SELECT 1 FROM ${this.table('market_service_messages')} m
           WHERE m.session_id = s.id
         )
       ORDER BY s.updated_at DESC, s.id DESC
       LIMIT 100;`,
      [account],
    );
    const ids = rows.map((row) => row.id);
    const messageRows = ids.length
      ? await this.db.query<MarketServiceMessageRow>(
          `SELECT *
           FROM ${this.table('market_service_messages')}
           WHERE session_id IN (${ids.map(() => '?').join(',')})
           ORDER BY session_id ASC, created_at ASC, id ASC;`,
          ids,
        )
      : [];
    const messagesBySession = messageRows.reduce<Record<number, MarketServiceMessageRow[]>>((acc, message) => {
      if (!acc[message.session_id]) acc[message.session_id] = [];
      acc[message.session_id].push(message);
      return acc;
    }, {});
    const mapped = rows.map((row) => this.mapServiceSession(row, messagesBySession[row.id] || []));
    const withTags = await this.attachServiceOrderTags(mapped);
    return { status: 200, message: 'Fetch success.', result: withTags };
  }

  async deleteServiceSession(req: any, body: any) {
    await this.ensureServiceTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const sessionId = Number(body?.sessionId || body?.id || 0);
    if (!sessionId) return { status: 400, message: '会话ID不能为空', result: null };

    const result: any = await this.db.query(
      `UPDATE ${this.table('market_service_sessions')}
       SET status = 0
       WHERE id = ? AND user_account = ? AND status = 1;`,
      [sessionId, account],
    );
    if (!result.affectedRows) return { status: 404, message: '会话不存在或已删除', result: null };
    return { status: 200, message: '会话已删除', result: { sessionId } };
  }

  async sendServiceMessage(req: any, body: any) {
    await this.ensureServiceTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const sessionId = Number(body?.sessionId || body?.id || 0);
    const content = String(body?.content || '').trim();
    const isProductMessage = body?.messageType === 'product' || Number(body?.messageType || 0) === 2;
    const rawPayload = body?.payload;
    const payload = isProductMessage ? this.normalizeServiceProductPayload(rawPayload) : null;
    const messageType = isProductMessage ? 2 : 1;
    if (!sessionId) return { status: 400, message: '会话ID不能为空', result: null };
    if (!content && !isProductMessage) return { status: 400, message: '消息内容不能为空', result: null };
    if (isProductMessage && !payload) return { status: 400, message: '商品消息缺少payload', result: null };
    if (content.length > 500) return { status: 400, message: '消息内容不能超过500个字符', result: null };

    const sessions = await this.db.query<{ id: number }>(
      `SELECT id FROM ${this.table('market_service_sessions')} WHERE id = ? AND user_account = ? AND status = 1 LIMIT 1;`,
      [sessionId, account],
    );
    if (!sessions.length) return { status: 404, message: '客服会话不存在', result: null };

    const result: any = await this.db.query(
      `INSERT INTO ${this.table('market_service_messages')} (session_id, sender_type, message_type, content, payload)
       VALUES (?, 1, ?, ?, ?);`,
      [sessionId, messageType, content, payload ? JSON.stringify(payload) : null],
    );

    if (isProductMessage && payload) {
      await this.db.query(
        `INSERT INTO ${this.table('market_service_messages')} (session_id, sender_type, message_type, content, payload)
         VALUES (?, 3, 2, ?, ?);`,
        [
          sessionId,
          '您好，已收到您发送的商品信息，商家客服稍后为您服务。',
          JSON.stringify(payload),
        ],
      );
    }

    await this.db.query(`UPDATE ${this.table('market_service_sessions')} SET updated_at = CURRENT_TIMESTAMP WHERE id = ?;`, [sessionId]);
    const rows = await this.db.query<MarketServiceMessageRow>(
      `SELECT * FROM ${this.table('market_service_messages')} WHERE id = ? LIMIT 1;`,
      [Number(result.insertId)],
    );
    return { status: 200, message: '发送成功', result: rows[0] ? this.mapServiceMessage(rows[0]) : null };
  }

  private queryServiceSessions(whereSql: string, params: any[] = []) {
    return this.db.query<MarketServiceSessionRow>(
      `SELECT s.*,
              COALESCE(u.shop_name, u.nickname, u.username, ms.username) AS shop_name,
              u.shop_avatar_url,
              ms.service_level AS shop_service_level,
              ms.sales_count AS shop_sales_count,
              p.name AS product_name,
              p.main_image_url AS product_image_url,
              p.min_price AS product_price
       FROM ${this.table('market_service_sessions')} s
       LEFT JOIN ${this.table('market_shops')} ms ON ms.id = s.shop_id AND ms.deleted_at IS NULL
       LEFT JOIN ${this.table('admin_users')} u ON u.username = ms.username
       LEFT JOIN ${this.table('products')} p ON p.id = s.product_id
       WHERE ${whereSql}
       LIMIT 1;`,
      params,
    );
  }

  async orders(req: any) {
    await this.ensureMarketOrderTables();
    await this.ensureReviewTable();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: [] };
    await this.expireOverdueUnpaidOrders(account);

    const rows = await this.db.query<MarketOrderRow>(
      `SELECT o.*,
              COALESCE(u.shop_name, u.nickname, u.username, s.username) AS shop_name,
              u.shop_avatar_url
       FROM ${this.table('market_orders')} o
       LEFT JOIN ${this.table('market_shops')} s ON s.id = o.shop_id AND s.deleted_at IS NULL
       LEFT JOIN ${this.table('admin_users')} u ON u.username = s.username
       WHERE o.user_account = ? AND o.deleted_at IS NULL
       ORDER BY o.created_at DESC, o.id DESC
       LIMIT 100;`,
      [account],
    );
    const ids = rows.map((row) => row.id);
    const itemRows = ids.length
      ? await this.db.query<MarketOrderItemRow>(
          `SELECT * FROM ${this.table('market_order_items')} WHERE order_id IN (${ids.map(() => '?').join(',')}) ORDER BY id ASC;`,
          ids,
        )
      : [];
    const itemsByOrder = itemRows.reduce<Record<number, MarketOrderItemRow[]>>((acc, item) => {
      if (!acc[item.order_id]) acc[item.order_id] = [];
      acc[item.order_id].push(item);
      return acc;
    }, {});
    const reviewedRows = ids.length
      ? await this.db.query<{ order_id: number }>(
          `SELECT order_id
           FROM ${this.table('market_product_reviews')}
           WHERE order_id IN (${ids.map(() => '?').join(',')}) AND deleted_at IS NULL
           GROUP BY order_id;`,
          ids,
        )
      : [];
    const reviewedOrderIds = new Set(reviewedRows.map((row) => Number(row.order_id)));
    return { status: 200, message: 'Fetch success.', result: rows.map((row) => this.mapOrder(row, itemsByOrder[row.id] || [], reviewedOrderIds)) };
  }

  async confirmReceipt(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const param = orderId || orderNo;
    const transactionResult = await this.db.transaction(async (query) => {
      const orderRows = await query<Array<{ id: number; shop_id: number | null; status: number }>>(
        `SELECT id, shop_id, status
         FROM ${this.table('market_orders')}
         WHERE ${whereSql} AND user_account = ? AND deleted_at IS NULL
         LIMIT 1 FOR UPDATE;`,
        [param, account],
      );
      const order = orderRows[0];
      if (!order || Number(order.status || 0) !== 30) return { ok: false };

      const updateRes: any = await query(
        `UPDATE ${this.table('market_orders')}
         SET status = 40, finished_at = NOW()
         WHERE id = ? AND status = 30 AND deleted_at IS NULL;`,
        [Number(order.id)],
      );
      if (!Number(updateRes?.affectedRows || 0)) return { ok: false };

      const itemRows = await query<Array<{ product_id: number | null; quantity: number }>>(
        `SELECT product_id, quantity
         FROM ${this.table('market_order_items')}
         WHERE order_id = ?;`,
        [Number(order.id)],
      );
      const productQtyMap = new Map<number, number>();
      let totalQuantity = 0;
      itemRows.forEach((item) => {
        const productId = Number(item.product_id || 0);
        const quantity = Math.max(0, Number(item.quantity || 0));
        if (!productId || !quantity) return;
        productQtyMap.set(productId, Number(productQtyMap.get(productId) || 0) + quantity);
        totalQuantity += quantity;
      });

      for (const [productId, quantity] of productQtyMap.entries()) {
        await query(
          `UPDATE ${this.table('products')}
           SET sold_count = sold_count + ?
           WHERE id = ? AND deleted_at IS NULL;`,
          [quantity, productId],
        );
      }

      const shopId = Number(order.shop_id || 0);
      if (shopId > 0 && totalQuantity > 0) {
        await query(
          `UPDATE ${this.table('market_shops')}
           SET sales_count = sales_count + ?
           WHERE id = ? AND deleted_at IS NULL;`,
          [totalQuantity, shopId],
        );
      }

      return { ok: true };
    });
    if (!transactionResult?.ok) return { status: 400, message: '只有待收货订单可以确认收货', result: null };
    return { status: 200, message: '确认收货成功', result: { status: this.statusTextFromOrder(40) } };
  }

  async cancelOrder(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const orderRows = await this.db.query<MarketOrderRow>(
      `SELECT id, status
       FROM ${this.table('market_orders')}
       WHERE ${whereSql} AND user_account = ? AND deleted_at IS NULL
       LIMIT 1;`,
      [orderId || orderNo, account],
    );
    const order = orderRows[0];
    if (!order) return { status: 404, message: '订单不存在', result: null };
    if (Number(order.status) !== 10) return { status: 400, message: '只有待付款订单可以取消', result: null };

    const cancelled = await this.cancelUnpaidOrder(Number(order.id));
    if (!cancelled) return { status: 400, message: '订单状态已变化，请刷新后重试', result: null };
    return { status: 200, message: '订单已取消', result: { status: this.statusTextFromOrder(50) } };
  }

  async updateOrderAddress(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    const address = this.normalizeReceiverSnapshot(body?.address || null);
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };
    if (!address || typeof address !== 'object') return { status: 400, message: '收货地址不能为空', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const result: any = await this.db.query(
      `UPDATE ${this.table('market_orders')}
       SET receiver_snapshot = ?
       WHERE ${whereSql} AND user_account = ? AND status IN (10, 20) AND deleted_at IS NULL;`,
      [JSON.stringify(address), orderId || orderNo, account],
    );
    if (!result.affectedRows) return { status: 400, message: '只有待付款或待发货订单可以修改地址', result: null };
    return { status: 200, message: '地址已修改', result: { address } };
  }

  async applyOrderRefund(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    const reason = String(body?.reason || '').trim();
    const receivedStatus = String(body?.receivedStatus || '').trim();
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };
    if (!reason) return { status: 400, message: '请选择退款原因', result: null };
    if (receivedStatus !== '已收货' && receivedStatus !== '未收货') return { status: 400, message: '请选择收货状态', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const result: any = await this.db.query(
      `UPDATE ${this.table('market_orders')}
       SET status = 60,
           refund_status = 1,
           refund_origin_status = status,
           refund_reason = ?,
           refund_received_status = ?,
           refund_applied_at = NOW(),
           refund_reviewed_at = NULL
       WHERE ${whereSql} AND user_account = ? AND status IN (20, 30) AND deleted_at IS NULL;`,
      [reason.slice(0, 300), receivedStatus, orderId || orderNo, account],
    );
    if (!result.affectedRows) return { status: 400, message: '只有待发货或待收货订单可以申请退款', result: null };
    return {
      status: 200,
      message: '退款申请已提交',
      result: {
        status: this.statusTextFromOrder(60),
        refundStatus: '商家审核中',
        refundReason: reason,
        refundReceivedStatus: receivedStatus,
      },
    };
  }

  async cancelOrderRefund(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const result: any = await this.db.query(
      `UPDATE ${this.table('market_orders')}
       SET status = CASE
             WHEN refund_origin_status IN (20, 30) THEN refund_origin_status
             WHEN shipped_at IS NULL THEN 20
             ELSE 30
           END,
           refund_status = NULL,
           refund_reviewed_at = NULL
       WHERE ${whereSql}
         AND user_account = ?
         AND status = 60
         AND (refund_status IS NULL OR refund_status IN (1, 3))
         AND deleted_at IS NULL;`,
      [orderId || orderNo, account],
    );
    if (!result.affectedRows) return { status: 400, message: '当前售后状态不能取消', result: null };
    return { status: 200, message: '售后已取消', result: { refundStatus: '' } };
  }

  async reviewOrderRefund(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    const action = String(body?.action || '').trim();
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };
    if (action !== 'approve' && action !== 'reject') return { status: 400, message: '处理动作无效', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const rows = await this.db.query<MarketOrderRow>(
      `SELECT id, status, refund_status, refund_origin_status, shipped_at
       FROM ${this.table('market_orders')}
       WHERE ${whereSql} AND user_account = ? AND deleted_at IS NULL
       LIMIT 1;`,
      [orderId || orderNo, account],
    );
    const order = rows[0];
    if (!order) return { status: 404, message: '订单不存在', result: null };
    if (Number(order.status) !== 60 || Number(order.refund_status || 0) !== 1) {
      return { status: 400, message: '当前售后状态不可处理', result: null };
    }

    if (action === 'approve') {
      const result: any = await this.db.query(
        `UPDATE ${this.table('market_orders')}
         SET status = 50,
             refund_status = 2,
             refund_reviewed_at = NOW(),
             cancelled_at = COALESCE(cancelled_at, NOW())
         WHERE id = ? AND user_account = ? AND status = 60 AND refund_status = 1 AND deleted_at IS NULL;`,
        [order.id, account],
      );
      if (!result.affectedRows) return { status: 400, message: '处理失败，请刷新后重试', result: null };
      return {
        status: 200,
        message: '已同意退款',
        result: {
          status: this.statusTextFromOrder(50),
          refundStatus: this.refundStatusText(2),
        },
      };
    }

    const rejectResult: any = await this.db.query(
      `UPDATE ${this.table('market_orders')}
       SET status = CASE
             WHEN refund_origin_status IN (20, 30) THEN refund_origin_status
             WHEN shipped_at IS NULL THEN 20
             ELSE 30
           END,
           refund_status = 3,
           refund_reviewed_at = NOW()
       WHERE id = ? AND user_account = ? AND status = 60 AND refund_status = 1 AND deleted_at IS NULL;`,
      [order.id, account],
    );
    if (!rejectResult.affectedRows) return { status: 400, message: '处理失败，请刷新后重试', result: null };
    const nextStatus = Number(order.refund_origin_status || 0) === 30 ? 30 : Number(order.refund_origin_status || 0) === 20 ? 20 : order.shipped_at ? 30 : 20;
    return {
      status: 200,
      message: '已拒绝退款',
      result: {
        status: this.statusTextFromOrder(nextStatus),
        refundStatus: this.refundStatusText(3),
      },
    };
  }

  async reviewOrder(req: any, body: any) {
    await this.ensureReviewTable();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };
    const orderId = Number(body?.orderId || body?.id || 0);
    const orderNo = String(body?.orderNo || '').trim();
    const content = String(body?.content || '').trim();
    const rating = Math.max(1, Math.min(5, Number(body?.rating || 5)));
    if (!orderId && !orderNo) return { status: 400, message: '订单ID不能为空', result: null };
    if (!content) return { status: 400, message: '评价内容不能为空', result: null };
    if (content.length > 500) return { status: 400, message: '评价内容不能超过500个字符', result: null };

    const whereSql = orderId ? 'id = ?' : 'order_no = ?';
    const orderRows = await this.db.query<MarketOrderRow>(
      `SELECT *
       FROM ${this.table('market_orders')}
       WHERE ${whereSql} AND user_account = ? AND deleted_at IS NULL
       LIMIT 1;`,
      [orderId || orderNo, account],
    );
    const order = orderRows[0];
    if (!order) return { status: 404, message: '订单不存在', result: null };
    if (Number(order.status) !== 40) return { status: 400, message: '确认收货后才能评价', result: null };

    const items = await this.db.query<MarketOrderItemRow>(
      `SELECT *
       FROM ${this.table('market_order_items')}
       WHERE order_id = ?
       ORDER BY id ASC;`,
      [order.id],
    );
    if (!items.length) return { status: 400, message: '订单商品不存在', result: null };

    const created: any[] = [];
    for (const item of items) {
      if (!item.product_id) continue;
      const snapshot: any = this.parseJsonValue(item.product_snapshot) || {};
      await this.db.query(
        `INSERT IGNORE INTO ${this.table('market_product_reviews')}
         (order_id, order_item_id, product_id, shop_id, user_account, user_name, rating, content)
         VALUES (?, ?, ?, ?, ?, ?, ?, ?);`,
        [
          order.id,
          item.id,
          item.product_id,
          order.shop_id ?? snapshot.shopId ?? null,
          account,
          this.maskAccount(account),
          rating,
          content,
        ],
      );
      created.push({ productId: item.product_id, orderItemId: item.id });
    }
    if (!created.length) return { status: 400, message: '没有可评价的商品', result: null };
    return { status: 200, message: '评价成功', result: created };
  }

  async createOrder(req: any, body: any) {
    await this.ensureMarketOrderTables();
    const account = await this.getAccountFromRequest(req);
    if (!account) return { status: 401, message: '登录已失效，请先登录。', result: null };

    const items = this.normalizeOrderItems(body?.items);
    if (!items.length) return { status: 400, message: '订单商品不能为空', result: null };
    const couponIds = this.normalizeOrderCouponIds(body);
    const couponsUsable = await this.validateUsableCoupons(account, couponIds);
    if (!couponsUsable) return { status: 400, message: '优惠券不可用或已被使用', result: null };
    const stockReserved = await this.reserveOrderStock(items);
    if (!stockReserved) return { status: 400, message: '库存不足，订单提交失败', result: null };

    const productAmount = Number(items.reduce((sum, item) => sum + item.totalAmount, 0).toFixed(2));
    const freightAmount = Math.max(0, Number(body?.shipping || body?.freight || 0));
    const discountAmount = Math.max(0, Math.min(productAmount + freightAmount, Number(body?.discount || 0)));
    const payAmount = Math.max(0, Number((productAmount + freightAmount - discountAmount).toFixed(2)));
    const paid = Boolean(body?.paid);
    const status = paid ? 20 : 10;
    const paymentMethod = paid ? String(body?.paymentMethod || 'mock') : null;
    const address = this.normalizeReceiverSnapshot(body?.address || null);
    const remark = String(body?.remark || '').slice(0, 300);

    const groups = new Map<string, typeof items>();
    items.forEach((item) => {
      const key = String(item.shopId ?? item.shop ?? 'default');
      groups.set(key, [...(groups.get(key) || []), item]);
    });

    const createdOrders: any[] = [];
    let groupIndex = 0;
    for (const groupItems of groups.values()) {
      const first = groupItems[0];
      const groupProductAmount = Number(groupItems.reduce((sum, item) => sum + item.totalAmount, 0).toFixed(2));
      const ratio = productAmount > 0 ? groupProductAmount / productAmount : 1;
      const groupDiscount = Number((discountAmount * ratio).toFixed(2));
      const groupPayAmount = Math.max(0, Number((groupProductAmount + freightAmount * ratio - groupDiscount).toFixed(2)));
      const orderNo = `MO${Date.now()}${String(groupIndex).padStart(2, '0')}${Math.floor(Math.random() * 1000).toString().padStart(3, '0')}`;
      const insertResult: any = await this.db.query(
        `INSERT INTO ${this.table('market_orders')}
         (order_no, user_id, user_account, shop_id, receiver_snapshot, product_amount, freight_amount, discount_amount, pay_amount, payment_method, remark, coupon_snapshot, status, paid_at)
         VALUES (?, 0, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ${paid ? 'NOW()' : 'NULL'});`,
        [
          orderNo,
          account,
          first.shopId,
          JSON.stringify(address),
          groupProductAmount,
          Number((freightAmount * ratio).toFixed(2)),
          groupDiscount,
          groupPayAmount,
          paymentMethod,
          remark,
          JSON.stringify({ couponIds }),
          status,
        ],
      );
      const orderId = Number(insertResult.insertId);
      for (const item of groupItems) {
        await this.db.query(
          `INSERT INTO ${this.table('market_order_items')}
           (order_id, product_id, sku_id, product_snapshot, quantity, unit_price, total_amount)
           VALUES (?, ?, ?, ?, ?, ?, ?);`,
          [
            orderId,
            Number(item.productId) || null,
            item.skuId,
            JSON.stringify(item),
            item.quantity,
            item.price,
            item.totalAmount,
          ],
        );
      }
      createdOrders.push({
        id: String(orderId),
        orderNo,
        status: this.statusTextFromOrder(status),
        shop: first.shop,
        shopId: first.shopId,
        shopAvatarUrl: first.shopAvatarUrl,
        items: groupItems,
        subtotal: groupProductAmount,
        discount: groupDiscount,
        total: groupPayAmount,
        address,
        createdAt: Date.now(),
        payDeadline: Date.now() + ORDER_PAY_TIMEOUT_MS,
      });
      if (!paid) await this.scheduleUnpaidOrderCancel(orderId);
      groupIndex += 1;
    }
    await this.markCouponsUsed(account, couponIds, createdOrders.map((order) => Number(order.id)));

    return { status: 200, message: '订单已生成', result: createdOrders };
  }

  async shop(idText: string, query: any) {
    const id = Number(idText);
    if (!Number.isFinite(id) || id <= 0) return { status: 400, message: 'Invalid shop id.', result: null };
    const shopRows = await this.db.query<any>(
      `SELECT s.id, s.username, s.service_level, s.fans_count, s.sales_count, s.rating, s.status,
              u.nickname, u.shop_name, u.shop_avatar_url, u.shop_description
       FROM ${this.table('market_shops')} s
       LEFT JOIN ${this.table('admin_users')} u ON u.username = s.username
       WHERE s.id = ? AND s.deleted_at IS NULL AND s.status = 1
       LIMIT 1;`,
      [id],
    );
    if (!shopRows.length) return { status: 404, message: 'Shop not found.', result: null };
    const products = await this.products({ ...query, shopId: id, limit: query?.limit || 50 });
    const shop = shopRows[0];
    return {
      status: 200,
      message: 'Fetch success.',
      result: {
        id: shop.id,
        username: shop.username,
        name: shop.shop_name || shop.nickname || shop.username,
        avatarUrl: shop.shop_avatar_url || '',
        description: shop.shop_description || '',
        serviceLevel: shop.service_level || '金牌客服',
        fans: Number(shop.fans_count || 0),
        sales: Number(shop.sales_count || 0),
        rating: Number(shop.rating || 5),
        products: products.result,
      },
    };
  }
}
