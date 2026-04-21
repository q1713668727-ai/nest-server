USE `server`;

CREATE TABLE IF NOT EXISTS `order` (
  `account` VARCHAR(255) NOT NULL
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COLLATE=utf8mb4_unicode_ci COMMENT='前台用户集市状态聚合表';

DELIMITER $$

DROP PROCEDURE IF EXISTS `add_order_column_if_missing`$$
CREATE PROCEDURE `add_order_column_if_missing`(
  IN p_column_name VARCHAR(64),
  IN p_column_definition TEXT
)
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM `information_schema`.`COLUMNS`
    WHERE `TABLE_SCHEMA` = DATABASE()
      AND `TABLE_NAME` = 'order'
      AND `COLUMN_NAME` = p_column_name
  ) THEN
    SET @ddl = CONCAT('ALTER TABLE `order` ADD COLUMN `', p_column_name, '` ', p_column_definition);
    PREPARE stmt FROM @ddl;
    EXECUTE stmt;
    DEALLOCATE PREPARE stmt;
  END IF;
END$$

DROP PROCEDURE IF EXISTS `add_order_primary_key_if_missing`$$
CREATE PROCEDURE `add_order_primary_key_if_missing`()
BEGIN
  IF NOT EXISTS (
    SELECT 1
    FROM `information_schema`.`TABLE_CONSTRAINTS`
    WHERE `TABLE_SCHEMA` = DATABASE()
      AND `TABLE_NAME` = 'order'
      AND `CONSTRAINT_TYPE` = 'PRIMARY KEY'
  ) THEN
    ALTER TABLE `order` ADD PRIMARY KEY (`account`);
  END IF;
END$$

DELIMITER ;

ALTER TABLE `order`
  MODIFY COLUMN `account` VARCHAR(255) NOT NULL COMMENT '用户账号，对应login.account';

CALL `add_order_primary_key_if_missing`();
CALL `add_order_column_if_missing`('market_collects', 'JSON NULL COMMENT ''集市商品收藏列表''');
CALL `add_order_column_if_missing`('market_cart', 'JSON NULL COMMENT ''集市购物车列表''');
CALL `add_order_column_if_missing`('market_wishlist', 'JSON NULL COMMENT ''集市心愿单列表''');
CALL `add_order_column_if_missing`('market_orders', 'JSON NULL COMMENT ''集市订单列表/订单号缓存''');
CALL `add_order_column_if_missing`('market_addresses', 'JSON NULL COMMENT ''集市收货地址列表''');
CALL `add_order_column_if_missing`('market_browse_history', 'JSON NULL COMMENT ''集市商品足迹''');
CALL `add_order_column_if_missing`('market_coupons', 'JSON NULL COMMENT ''集市优惠券列表''');
CALL `add_order_column_if_missing`('created_at', 'DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP COMMENT ''创建时间''');
CALL `add_order_column_if_missing`('updated_at', 'DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP COMMENT ''更新时间''');

DROP PROCEDURE IF EXISTS `add_order_column_if_missing`;
DROP PROCEDURE IF EXISTS `add_order_primary_key_if_missing`;
