# ecommerce-lab · 电商秒杀高并发实验

用 Go + Redis + MySQL 把电商复习里的核心链路落成可运行代码：**多级缓存 / 缓存穿透击穿雪崩 / 分片库存 + Lua 原子扣减 / 判重 / 事务消息 / 支付超时关单归还 / 两级限流**，并自带压测工具验证**零超卖、零少卖**。

## 目录结构

```
ecommerce-lab/
├── schema.sql                  # 7 张表 + 索引设计（首次启动自动导入）
├── docker-compose.yml          # Redis 5.0.3 + MySQL 8.0.13（端口 6380/3307，不冲突本机）
├── cmd/
│   ├── server/                 # HTTP 服务入口
│   ├── bench/                  # 压测：detail / seckill / stress（含 QPS、P99、超卖校验）
│   └── cleanup/                # 活动结束后按前缀 SCAN + UNLINK 清理业务 key
└── internal/
    ├── config/                 # 配置（全部支持环境变量覆盖）
    ├── store/                  # MySQL + Redis 客户端
    ├── cache/                  # 本地 LRU / 布隆过滤 / singleflight / TTL 抖动
    ├── ratelimit/              # 本地令牌桶 + Redis 滑动窗口 Lua
    ├── seckill/                # 分片扣减 Lua / 事务消息 / 关单归还
    └── handler/                # HTTP 接口
```

## 快速开始

依赖：Go 1.23+、Docker。

```bash
# 1. 起 Redis + MySQL（schema.sql 首次自动导入）
docker compose up -d

# 2. 起服务
go run ./cmd/server

# 3. 冒烟
curl http://127.0.0.1:8080/health
curl 'http://127.0.0.1:8080/api/detail?id=1001'    # 多级缓存
curl 'http://127.0.0.1:8080/api/init?total=1000'   # 重置秒杀库存
curl 'http://127.0.0.1:8080/api/seckill?user_id=1' # 秒杀一单
curl http://127.0.0.1:8080/api/stats               # 缓存命中率 + 桶级库存 + 订单数
```

默认连接（见 `internal/config/config.go`，均可用环境变量覆盖）：

| 组件 | 默认值 | 覆盖变量 |
|---|---|---|
| MySQL | `lab:lab123@tcp(127.0.0.1:3307)/ecomm_lab` | `MYSQL_DSN` |
| Redis | `127.0.0.1:6380` | `REDIS_ADDR` / `REDIS_PWD` |
| 监听 | `:8080` | `LISTEN_ADDR` |
| 支付超时 | `300s` | `PAY_TIMEOUT_S` |

## 接口

| 路径 | 说明 |
|---|---|
| `GET /health` | 健康检查 |
| `GET /api/detail?id=1001` | 商品详情：本地缓存 → Redis → DB，含布隆/空值缓存/singleflight/降级 |
| `GET /api/seckill?user_id=1` | 秒杀下单：限流 → Lua 扣减 → 本地消息表，异步落库 |
| `GET /api/init?total=1000` | 重置库存：清空 Redis 与订单表，按 10 桶均分重建 |
| `GET /api/stats` | 缓存命中率、Redis 剩余库存、桶级分布、DB 订单数 |

## 秒杀链路

1. **分片库存**：`total` 均分到 10 个 key（`seckill:stock:{act}:{bucket}`），避免单 key 热点。
2. **Lua 原子扣减**：判重 `SETNX`（一人一次）→ 从轮询起点**依次探测所有分片，扣第一个有货的桶**（空桶直接跳过，不做空轮询）→ 写扣减流水 `seckill:flow:{orderNo}`。
3. **事务消息**：扣减成功后写本地消息表 `local_message`，Worker 异步转成 `seckill_order` / `inventory_log` / `user_qualification`。
4. **超时归还**：Worker 按 `PAY_TIMEOUT_S` 关单，`restoreStockLua` 把库存**还回原桶**、释放资格、冲销流水。
5. **两级限流**：本地令牌桶挡第一层，Redis 滑动窗口 Lua 做集群级配额。

### 修复空轮询（本仓库的一个实战点）

早期实现由 Go 侧轮询选一个桶，桶空了直接返回 `410`——10 个桶里 2 个空时，约 20% 的请求配额被白白浪费。
现在把"选有货的桶"放进 Lua：一次 RTT 内按序探测全部分片，跳到第一个有货的桶扣减。这样：

- 空桶不再占用请求配额；
- `410` 只表示**全局售罄**，不再与"撞空桶"混淆；
- 顺带消除了"Go 侧选桶时还有货、扣减时被并发抢空"的竞态。

## 压测

```bash
# 详情页读：100 并发 / 1 万请求
go run ./cmd/bench -c 100 -n 10000 -mode detail

# 秒杀写：200 并发 / 2000 请求抢 1000 件
curl 'http://127.0.0.1:8080/api/init?total=1000'
go run ./cmd/bench -c 200 -n 2000 -mode seckill

# 不均匀场景回归：验证空桶被跳过、归还后库存回原桶
#   server 需用较短的支付超时启动，否则要等 5 分钟才关单
PAY_TIMEOUT_S=5 go run ./cmd/server
go run ./cmd/bench -mode stress
```

`stress` 模式会依次执行：重置库存 → 第一轮抢购 → 等超时归还 → 强制打空 2 个桶 → 第二轮抢购 → 等归还 → 第三轮抢购，并校验：

| 轮次 | 期望 |
|---|---|
| 第一轮 | 1000 请求 / 1000 库存 → `200=1000, 410=0` |
| 第二轮 | 1200 请求 / 800 库存 → `200=800, 410=400`（全部真售罄，不浪费在空桶） |
| 第三轮 | 400 请求 / 800 库存 → `200=400, 410=0`（空桶自动跳过） |
| 对账 | `Redis 剩余 + DB 订单 = 实际库存`，零超卖零少卖 |

> 秒杀是异步落库，1000 单约需 10~30 秒才追平（MySQL 默认 `innodb_flush_log_at_trx_commit=1 + sync_binlog=1`，本机实测 ~100 单/秒）。压测脚本已改为轮询 `/api/stats` 等账平再判定，不用手工 sleep。

## Redis key 生命周期

| key | TTL 策略 | 清理方式 |
|---|---|---|
| `static:{id}` / `dynamic:{id}` / `NULL` / `rl:{biz}` | 短 TTL（10min / 60s / 30s / 2×窗口） | 自动过期 |
| `seckill:stock:{act}:{bucket}` | 无短 TTL + 兜底 `EXPIREAT`（活动结束 + 30 天） | `cmd/cleanup` 按活动前缀 SCAN + UNLINK |
| `seckill:user:{act}:{uid}` | 同上 | 关单归还时 DEL，或活动级清理 |
| `seckill:flow:{orderNo}` | 同上 | 归还时 UNLINK（DB 已落 `inventory_log`） |

秒杀业务 key **不设短 TTL**：库存到点自动过期 = 超卖，资格到点自动过期 = 重复抢。改用"活动级清理 + 超长兜底 EXPIREAT"，清理任务挂了 Redis 也会在活动结束 30 天后自愈。

## 实测数据（macOS / Go 1.23 / Redis 5 / MySQL 8）

| 场景 | 并发 | QPS | P50 | P99 | 结果 |
|---|---|---|---|---|---|
| 详情页读 | 100 | ~2.8w | 2.7ms | 26ms | 零错误，本地缓存命中 ~99% |
| 秒杀写 | 200 | ~8900 | 21ms | 40ms | 1000 成功 / 1000 售罄，零超卖零少卖 |

更详细的实验步骤与「复习点 → 代码位置」对照见 [RUN.md](RUN.md)。
