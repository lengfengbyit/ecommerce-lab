# 电商导购架构 · 本地实战（Go + Redis + MySQL）

把复习里的电商知识点落成可运行代码：**多级缓存 / 缓存三兄弟 / 秒杀分片 / 判重 / 事务消息 / 归还回原桶 / 两级限流**。

## 目录结构

```
ecommerce-lab/
├── schema.sql                     # 数据库设计（7 张表 + 索引设计说明）
├── docker-compose.yml             # 一键起 Redis + MySQL（可选）
├── cmd/
│   ├── server/main.go             # HTTP 服务入口
│   ├── seed/main.go               # 数据预热
│   └── bench/main.go              # 压测脚本（QPS/P99/超卖校验）
└── internal/
    ├── config/                    # 配置（环境变量覆盖）
    ├── store/                     # MySQL + Redis 客户端
    ├── cache/                     # 多级缓存：本地 LRU / 布隆 / singleflight / TTL 抖动
    ├── ratelimit/                 # 本地令牌桶 + Redis 滑动窗口 Lua
    ├── seckill/                   # 秒杀引擎：Lua 扣减 / 分片 / 事务消息 / 关单归还
    └── handler/                   # HTTP 接口
```

## 启动

### 1. 起 Redis + MySQL

项目自带 `docker-compose.yml`，镜像版本与本机现有环境一致（Redis 5.0.3 / MySQL 8.0.13），**端口错开不冲突**：

```bash
docker compose up -d
```

- Redis：宿主 `6380` → 容器 `6379`
- MySQL：宿主 `3307` → 容器 `3306`
- `schema.sql` 挂载到 `/docker-entrypoint-initdb.d/`，**首次启动自动导入**（已有数据卷不会重复导）

> 端口为什么错开：本机 dnmp 的 Redis/MySQL 占了 6379/3306，项目复用会端口冲突。
> 想用本机已有服务，用环境变量覆盖即可：
> `MYSQL_DSN='lab:lab123@tcp(127.0.0.1:3306)/ecomm_lab?charset=utf8mb4&parseTime=True&loc=Local' REDIS_ADDR=127.0.0.1:6379 go run ./cmd/server`

停止：`docker compose down`（不加 `-v`，数据保留）。

连接参数（默认值在 `internal/config/config.go`）：
- MySQL：`lab:lab123@tcp(127.0.0.1:3307)/ecomm_lab`（root 密码 `123456`）
- Redis：`127.0.0.1:6380`

> 如果用 WorkBuddy 资料库的「SQL 导入」功能（只接受字面量值）：
> `schema.sql` 里的 `images` 字段已是 JSON 字符串字面量，不用 `JSON_ARRAY()`，可直接导入。

### 2. 起服务

```bash
go run ./cmd/server
```

### 3. 验证接口

```bash
curl http://127.0.0.1:8080/health
curl http://127.0.0.1:8080/api/detail?id=1001       # 详情页多级缓存
curl http://127.0.0.1:8080/api/detail?id=999999     # 穿透闸门 → 404
curl http://127.0.0.1:8080/api/stats               # 缓存命中 + 库存对账
```

## 压测

```bash
# 详情页读：100 并发 / 1 万请求
go run ./cmd/bench -c 100 -n 10000 -mode detail

# 秒杀写：200 并发 / 2000 请求抢 1000 件（先重置库存）
curl 'http://127.0.0.1:8080/api/init?total=1000'
go run ./cmd/bench -c 200 -n 2000 -mode seckill
```

压测脚本输出：QPS、P50/P95/P99、状态码分布、错误分类，秒杀场景额外校验**超卖/少卖**。

注意异步落库的等待时间：秒杀成功后订单由 Worker 异步落库，**1000 单大约需要 10~30 秒**才能追平——每单一个事务，MySQL 默认 `innodb_flush_log_at_trx_commit=1` + `sync_binlog=1`，本机实测吞吐只有 ~100 单/秒。
- `checkOversell` 已改为轮询 `/api/stats` 直到「Redis 剩余 + DB 订单 = 应有库存」（最长 120s）再判定，**不用手工 sleep**；
- 手工 `curl /api/stats` 时看到"少卖"是正常的中间态，并不是真的丢单，等 Worker 追上再看。

# 活动结束后清理业务 key（SCAN + UNLINK，按活动前缀，不误伤其他活动）
go run ./cmd/cleanup
# 输出各前缀删除数，并自动二次运行验证幂等（都应归零）
# 可用 ACTIVITY_END_AT=2026-11-30T23:59:59+08:00 调整兜底 EXPIREAT 的基准（默认 24h 后 + 30 天）

## Redis key 生命周期（为什么有的永不过期）

| key | TTL 策略 | 谁清理 |
|---|---|---|
| `static:{id}` / `dynamic:{id}` / `NULL` / `rl:{biz}` | 短 TTL（10min / 60s / 30s / 2×窗口） | Redis 惰性删除 + 定期抽样，自动回收 |
| `seckill:stock:{act}:{bucket}` | 无短 TTL + 兜底 EXPIREAT（活动结束+30天） | 活动级 `cmd/cleanup`；库存是业务 state，短 TTL 过期=超卖 |
| `seckill:user:{act}:{uid}` | 同上 | 关单归还时 DEL，或活动级清理；短 TTL 过期=能重复抢 |
| `seckill:flow:{orderNo}` | 同上 | 归还时 UNLINK（DB 已落 inventory_log），或活动级清理 |

**为什么秒杀 key 不设短 TTL**：业务 state 到点被自动删就是事故——库存重置→超卖、资格释放→重复抢、流水丢失→无法回原桶对账。改用「活动级清理 + 超长兜底 EXPIREAT」：清理任务按活动结束后跑，任务挂了 Redis 也会在活动结束后 30 天自愈,且这段时间远超活动时长,不会误删。

**为什么用 SCAN + UNLINK 而不是 KEYS + DEL**：KEYS 一次遍历全库阻塞主线程;DEL 大 key 同步释放内存也阻塞;UNLINK 只摘 key 链,内存回收丢后台线程。

## 复习点 → 代码位置对照

| 复习知识点 | 代码位置 |
|---|---|
| 多级缓存 CDN→本地→Redis→DB | `internal/cache/service.go` `GetDetail()` |
| 静态/动态字段分离 | `ProductStatic` / `ProductDynamic` 结构体 |
| 缓存穿透（布隆 + 空值缓存） | `internal/cache/bloom.go`、`service.go` 的 `NULL` 缓存 |
| 缓存击穿（singleflight） | `internal/cache/service.go` 的 `singleflight` |
| 缓存雪崩（TTL 随机抖动） | `jitterTTL()` |
| 降级返旧值 | `GetDetail()` 的 degraded 分支 |
| 两级限流 | `internal/ratelimit/limiter.go`（本地令牌桶 + Redis Lua 滑动窗口） |
| 库存分片（10 key × 100，轮询起点 + Lua 内跳过空桶选货） | `internal/seckill/engine.go` `InitStock()` / `pickBucket()`、`seckillDeductLua` |
| 判重 SETNX（失败不释放） | `internal/seckill/lua.go` `seckillDeductLua` |
| 事务消息（扣减与消息同成败） | `writeLocalMessage()` + `internal/seckill/worker.go` |
| 归还回原桶（冲销流水） | `restoreStockLua` + `Restore()` |
| 事件驱动即时还（超时关单） | `worker.go` `closeTimeoutOrders()` |
| key 兜底 EXPIREAT（活动结束+30天） | `config.go` `BackupExpireAt()`、`engine.go` `InitStock()` / `lua.go` |
| 活动级批量清理（SCAN + UNLINK） | `engine.go` `Cleanup()`、`cmd/cleanup/main.go` |
| 归还时 UNLINK 流水（DB 已持久化） | `restoreStockLua` |

## 实测数据（本机 macOS / Go 1.23 / Redis 5 / MySQL 8）

| 场景 | 并发 | QPS | P50 | P99 | 结果 |
|---|---|---|---|---|---|
| 详情页读 | 100 | ~2.8w | 2.7ms | 26ms | 零错误，本地缓存命中 ~99% |
| 秒杀写 | 200 | ~8900 | 21ms | 40ms | 1000 成功 / 1000 售罄，零超卖零少卖 |
