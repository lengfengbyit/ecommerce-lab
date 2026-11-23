package ratelimit

import (
	"context"
	"fmt"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

// ============================================================================
// L1 · 本地令牌桶：挡住绝大部分突发本机流量，零网络开销
// ============================================================================
type TokenBucket struct {
	mu       sync.Mutex
	tokens   float64
	capacity float64
	rate     float64 // 每秒补充令牌数
	last     time.Time
}

func NewTokenBucket(rate, capacity float64) *TokenBucket {
	return &TokenBucket{tokens: capacity, capacity: capacity, rate: rate, last: time.Now()}
}

func (tb *TokenBucket) Allow() bool {
	tb.mu.Lock()
	defer tb.mu.Unlock()
	now := time.Now()
	elapsed := now.Sub(tb.last).Seconds()
	tb.tokens += elapsed * tb.rate
	if tb.tokens > tb.capacity {
		tb.tokens = tb.capacity
	}
	tb.last = now
	if tb.tokens < 1 {
		return false
	}
	tb.tokens--
	return true
}

// ============================================================================
// L2 · 分布式滑动窗口：Redis + Lua 保证原子，兜跨实例全局配额
// 对应复习点：滑动窗口日志法精确但耗内存、计数器法是加权估算；
//             这里用 ZSET 日志法（精确版），窗口 1s。
// 为什么必须 Lua：判断+写入+删除必须原子，否则并发下会超发配额。
// ============================================================================
const slideWindowLua = `
local key = KEYS[1]
local now = tonumber(ARGV[1])
local window = tonumber(ARGV[2])
local limit = tonumber(ARGV[3])
redis.call('ZREMRANGEBYSCORE', key, 0, now - window)
local count = redis.call('ZCARD', key)
if count >= limit then
  return 0
end
redis.call('ZADD', key, now, now .. '-' .. math.random(1, 1000000))
redis.call('PEXPIRE', key, window * 2)
return 1
`

type DistributedLimiter struct {
	rdb    *redis.Client
	script *redis.Script
	limit  int
	window time.Duration
}

func NewDistributedLimiter(rdb *redis.Client, limit int, window time.Duration) *DistributedLimiter {
	return &DistributedLimiter{
		rdb: rdb, script: redis.NewScript(slideWindowLua),
		limit: limit, window: window,
	}
}

func (d *DistributedLimiter) Allow(ctx context.Context, biz string) (bool, error) {
	key := fmt.Sprintf("rl:%s", biz)
	now := time.Now().UnixMilli()
	res, err := d.script.Run(ctx, d.rdb, []string{key},
		now, d.window.Milliseconds(), d.limit).Int()
	if err != nil {
		return false, err
	}
	return res == 1, nil
}
