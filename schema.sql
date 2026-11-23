-- ============================================================================
-- 电商导购架构 · MySQL 数据库设计
-- 配套复习：详情页多级缓存 / 静态动态字段分离 / 秒杀库存扣减 / 归还回原桶
-- MySQL 8.0+  字符集 utf8mb4
-- ============================================================================

CREATE DATABASE IF NOT EXISTS ecomm_lab DEFAULT CHARACTER SET utf8mb4 COLLATE utf8mb4_unicode_ci;
USE ecomm_lab;

-- ----------------------------------------------------------------------------
-- 1. 商品主数据表：只存【静态字段】（标题/图片/详情）
--    对应复习点：静态字段可长 TTL 缓存在 CDN + Caffeine，改动很少
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS product;
CREATE TABLE product (
  id            BIGINT       NOT NULL COMMENT '商品ID',
  title         VARCHAR(200) NOT NULL COMMENT '商品标题',
  sub_title     VARCHAR(300) DEFAULT '' COMMENT '副标题',
  images        JSON         COMMENT '图片列表（CDN URL）',
  detail        TEXT         COMMENT '商品详情 HTML 片段（可整体推 CDN 做页面静态化）',
  category_id   BIGINT       DEFAULT 0 COMMENT '类目ID',
  status        TINYINT      NOT NULL DEFAULT 1 COMMENT '1上架 0下架',
  version       INT          NOT NULL DEFAULT 0 COMMENT '乐观锁版本号，配合 binlog 删缓存',
  created_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at    DATETIME     NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_category_status (category_id, status) COMMENT '类目页列表查询'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='商品主数据（静态字段）';

-- ----------------------------------------------------------------------------
-- 2. SKU 表：存【动态字段】（价格 / DB 库存基线）
--    对应复习点：价格绝对不能错；库存以 Redis 分片为准，DB 只做最终对账基线
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS sku;
CREATE TABLE sku (
  id            BIGINT        NOT NULL COMMENT 'SKU ID',
  product_id    BIGINT        NOT NULL COMMENT '所属商品',
  spec          VARCHAR(200)  DEFAULT '' COMMENT '规格描述',
  price_cents   INT           NOT NULL COMMENT '价格（分），动态字段，独立接口直读 Redis',
  stock         INT           NOT NULL DEFAULT 0 COMMENT 'DB 库存基线（对账用，不扛秒杀流量）',
  version       INT           NOT NULL DEFAULT 0 COMMENT '乐观锁',
  updated_at    DATETIME      NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  KEY idx_product (product_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='SKU 动态字段（价格/库存基线）';

-- ----------------------------------------------------------------------------
-- 3. 秒杀活动表：库存总量 + 时间窗 + 分片配置
--    对应复习点：缩短支付时限（秒杀 5-10min，不照搬普通电商 15min）
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS seckill_activity;
CREATE TABLE seckill_activity (
  id              BIGINT      NOT NULL COMMENT '活动ID',
  sku_id          BIGINT      NOT NULL COMMENT '秒杀SKU',
  total_stock     INT         NOT NULL COMMENT '秒杀总库存（件）',
  bucket_count    INT         NOT NULL DEFAULT 10 COMMENT '库存分片数（10 key × 100 件打散热点）',
  pay_timeout_sec INT         NOT NULL DEFAULT 300 COMMENT '支付超时秒数（5分钟，防黄牛占货）',
  start_at        DATETIME    NOT NULL COMMENT '开始时间',
  end_at          DATETIME    NOT NULL COMMENT '结束时间',
  status          TINYINT     NOT NULL DEFAULT 1 COMMENT '1进行中 0结束',
  PRIMARY KEY (id),
  KEY idx_sku_status (sku_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀活动配置';

-- ----------------------------------------------------------------------------
-- 4. 库存流水表：【归还回原桶】的核心
--    每次扣减写一条流水，记下 bucket_no；关单归还必须按 bucket_no 精确冲销。
--    对应复习点：归还=冲销流水，不是随便补一件货。
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS inventory_log;
CREATE TABLE inventory_log (
  id          BIGINT      NOT NULL AUTO_INCREMENT,
  activity_id BIGINT      NOT NULL COMMENT '活动ID',
  user_id     BIGINT      NOT NULL COMMENT '用户ID',
  order_no    VARCHAR(64) NOT NULL COMMENT '订单号（幂等键）',
  bucket_no   INT         NOT NULL COMMENT '扣减的分片桶号 —— 归还必须回这个桶',
  qty         INT         NOT NULL DEFAULT 1 COMMENT '扣减数量',
  op_type     TINYINT     NOT NULL COMMENT '1扣减 2归还',
  settled     TINYINT     NOT NULL DEFAULT 0 COMMENT '是否已核销：扣减后归还置1，防重复归还',
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_order_op (order_no, op_type) COMMENT '同一订单同一操作只能一次，幂等',
  KEY idx_activity_user (activity_id, user_id),
  KEY idx_settled (settled, op_type) COMMENT '对账扫描：未核销的扣减流水'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='库存扣减/归还流水（归还回原桶依据）';

-- ----------------------------------------------------------------------------
-- 5. 用户资格表（判重）：一人一次，抢到即锁定
--    对应复习点：资格抢到即锁定、失败不释放、重复请求直接拒；
--               只有超时关单/主动取消才释放。Redis SETNX 是热路径，本表是落库兜底。
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS user_qualification;
CREATE TABLE user_qualification (
  id          BIGINT      NOT NULL AUTO_INCREMENT,
  activity_id BIGINT      NOT NULL,
  user_id     BIGINT      NOT NULL,
  status      TINYINT     NOT NULL COMMENT '1已抢到(锁定) 2已释放(关单)',
  order_no    VARCHAR(64) DEFAULT '' COMMENT '关联订单号',
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_activity_user (activity_id, user_id) COMMENT '核心：同一活动同一用户唯一，防重复抢',
  KEY idx_status (activity_id, status)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='用户秒杀资格（判重落库兜底）';

-- ----------------------------------------------------------------------------
-- 6. 秒杀订单表
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS seckill_order;
CREATE TABLE seckill_order (
  order_no    VARCHAR(64) NOT NULL COMMENT '订单号（业务幂等键）',
  activity_id BIGINT      NOT NULL,
  sku_id      BIGINT      NOT NULL,
  user_id     BIGINT      NOT NULL,
  qty         INT         NOT NULL DEFAULT 1,
  pay_amount  INT         NOT NULL COMMENT '支付金额（分）',
  bucket_no   INT         NOT NULL COMMENT '来源分片桶（追溯用）',
  status      TINYINT     NOT NULL COMMENT '1待支付 2已支付 3已关闭',
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (order_no),
  UNIQUE KEY uk_activity_user (activity_id, user_id) COMMENT '一单一用户，防重复下单',
  KEY idx_status_time (status, created_at) COMMENT '超时关单扫描',
  KEY idx_activity (activity_id)
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='秒杀订单';

-- ----------------------------------------------------------------------------
-- 7. 本地消息表：模拟「事务消息」——扣减库存与下单消息同成功同失败
--    对应复习点：扣减与发消息要原子，否则少卖/超卖；worker 扫表异步下单。
-- ----------------------------------------------------------------------------
DROP TABLE IF EXISTS local_message;
CREATE TABLE local_message (
  id          BIGINT      NOT NULL AUTO_INCREMENT,
  order_no    VARCHAR(64) NOT NULL,
  activity_id BIGINT      NOT NULL,
  sku_id      BIGINT      NOT NULL,
  user_id     BIGINT      NOT NULL,
  bucket_no   INT         NOT NULL,
  qty         INT         NOT NULL DEFAULT 1,
  pay_amount  INT         NOT NULL,
  status      TINYINT     NOT NULL DEFAULT 0 COMMENT '0待处理 1已下单 2已放弃',
  retry_count INT         NOT NULL DEFAULT 0,
  created_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP,
  updated_at  DATETIME    NOT NULL DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
  PRIMARY KEY (id),
  UNIQUE KEY uk_order (order_no),
  KEY idx_status_created (status, created_at) COMMENT 'worker 扫描待处理消息'
) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4 COMMENT='本地消息表（事务消息模拟）';

-- ============================================================================
-- 初始化示例数据：1 个商品、1 个 SKU、1 个秒杀活动（1000 件 / 10 桶 / 5 分钟）
-- ============================================================================
INSERT INTO product (id, title, sub_title, images, detail, category_id)
VALUES (1001, '无线降噪耳机 Pro', '主动降噪 · 40小时续航',
        '["https://cdn.example.com/p/1001/1.jpg","https://cdn.example.com/p/1001/2.jpg"]',
        '<h3>旗舰降噪</h3><p>40mm 动圈，深度降噪 45dB。</p>', 10);

INSERT INTO sku (id, product_id, spec, price_cents, stock)
VALUES (2001, 1001, '曜石黑', 99900, 1000);

INSERT INTO seckill_activity (id, sku_id, total_stock, bucket_count, pay_timeout_sec, start_at, end_at)
VALUES (3001, 2001, 1000, 10, 300, '2026-01-01 00:00:00', '2099-12-31 23:59:59');
