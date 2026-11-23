package seckill

// ============================================================================
// 秒杀 Lua 脚本集
// 所有脚本都在 Redis 单线程里原子执行，杜绝并发下的超卖/重复抢。
// ============================================================================

// seckillDeductLua 扣减库存（含判重 + 跨桶选货）
// KEYS[1]          = 判重 key      seckill:user:{activity}:{userId}  (SETNX，抢到即锁定)
// KEYS[2..n+1]     = 各分片库存 key，按调用方给的尝试顺序排列（通常是轮询起点开始）
// ARGV[1] = orderNo    ARGV[2] = userId    ARGV[3] = 兜底过期时间戳(秒)
// 返回：-1 重复抢；0 所有桶都空；N>0 成功，扣的是第 N 个库存 key（1-based 尝试序号）
//
// 关键：选货在 Lua 内一次完成——从起点按序探测所有桶，跳到第一个有货的桶扣减。
// 这样"空桶"不会再返回 410 浪费请求配额（修复空轮询），也不会出现
// "Go 侧选桶时还有货、扣的时候被并发抢空" 的竞态。
//
// 注意：判重 key 不设"短 TTL"（到点删=能重复抢），但挂一个活动结束+30天的
//
//	EXPIREAT 兜底——清理任务挂了 Redis 也会回收，且远超活动时长不会误删。
const seckillDeductLua = `
local userKey  = KEYS[1]
local orderNo  = ARGV[1]
local userId   = ARGV[2]
local expireAt = ARGV[3]

-- ① 判重：一人一次，SETNX 抢到即锁定（失败直接返回，不删 key）
if redis.call('SETNX', userKey, orderNo) == 0 then
  return -1
end
-- 兜底 EXPIREAT：不是"短 TTL"，是活动结束+30天的绝对过期，活动期间绝不会误删
redis.call('EXPIREAT', userKey, expireAt)

-- ② 从起点按序探测各分片，扣第一个有货的桶（空桶直接跳过，不空轮询）
for i = 2, #KEYS do
  local stock = tonumber(redis.call('GET', KEYS[i]) or '0')
  if stock > 0 then
    redis.call('DECR', KEYS[i])

    -- ③ 写扣减流水到 Redis（异步 worker 落库 + 写本地消息表）
    -- 流水里记 bucket 号，这是"归还回原桶"的依据
    local bucket = string.match(KEYS[i], ':(%d+)$')
    local flowKey = 'seckill:flow:' .. orderNo
    redis.call('HSET', flowKey,
      'orderNo', orderNo, 'userId', userId,
      'bucket', bucket, 'qty', '1', 'status', 'deducted')
    redis.call('EXPIREAT', flowKey, expireAt)
    return i - 1
  end
end

-- ③ 所有桶都空：归还刚抢到的资格（这里允许释放是因为压根没扣到库存）
redis.call('DEL', userKey)
return 0
`

// restoreStockLua 归还库存（关单时调用）
// KEYS[1] = 分片库存 key（必须是流水里记的原桶）
// KEYS[2] = 判重 key
// KEYS[3] = 流水 key
// ARGV[1] = orderNo
// 返回：1成功 0重复归还（幂等）
const restoreStockLua = `
local stockKey = KEYS[1]
local userKey  = KEYS[2]
local flowKey  = KEYS[3]
local orderNo  = ARGV[1]

-- 幂等：流水状态必须是 deducted，已 restored 直接返回 0
local status = redis.call('HGET', flowKey, 'status')
if status ~= 'deducted' then
  return 0
end

-- 原子归还：回【原桶】+ 释放资格 + UNLINK 流水（DB 已持久化 inventory_log，Redis 这份不再留）
-- 为什么这里删而不是落库后删：Restore 靠 status=deducted 判幂等，
-- 落库后提前删会让"超时关单"误判为已归还，库存就漏还了。归还这一刻删才安全。
redis.call('INCR', stockKey)
redis.call('DEL', userKey)
redis.call('UNLINK', flowKey)
return 1
`
