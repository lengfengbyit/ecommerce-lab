package seckill

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrSoldOut   = errors.New("已售罄")
	ErrDuplicate = errors.New("重复抢购")
)

type Engine struct {
	db       *sql.DB
	rdb      *redis.Client
	actID    int64
	skuID    int64
	buckets  int
	expireAt time.Time // 兜底绝对过期时间（活动结束 + N 天）
	deduct   *redis.Script
	restore  *redis.Script
	mu       sync.Mutex
	nextB    int // 轮询选桶起点
}

func NewEngine(db *sql.DB, rdb *redis.Client, actID, skuID int64, buckets int, expireAt time.Time) *Engine {
	e := &Engine{
		db: db, rdb: rdb, actID: actID, skuID: skuID, buckets: buckets,
		expireAt: expireAt,
		deduct:   redis.NewScript(seckillDeductLua),
		restore:  redis.NewScript(restoreStockLua),
	}
	return e
}

func (e *Engine) stockKey(bucket int) string {
	return fmt.Sprintf("seckill:stock:%d:%d", e.actID, bucket)
}
func (e *Engine) userKey(uid int64) string {
	return fmt.Sprintf("seckill:user:%d:%d", e.actID, uid)
}
func (e *Engine) flowKey(orderNo string) string {
	return fmt.Sprintf("seckill:flow:%s", orderNo)
}

// InitStock 初始化分片库存：1000 件拆 10 桶 × 100 件。
// 关键：随机路由选桶，绝不按 user_id hash 绑桶（否则少卖）。
func (e *Engine) InitStock(ctx context.Context, total int) error {
	per := total / e.buckets
	remain := total % e.buckets
	pipe := e.rdb.Pipeline()
	for i := 0; i < e.buckets; i++ {
		qty := per
		if i < remain {
			qty++
		}
		key := e.stockKey(i)
		pipe.Set(ctx, key, qty, 0)
		// 兜底 EXPIREAT：库存是业务 state，不能设短 TTL（过期=重置=超卖），
		// 但挂活动结束+30天的绝对过期，防清理任务失效导致永久泄漏。
		pipe.ExpireAt(ctx, key, e.expireAt)
	}
	_, err := pipe.Exec(ctx)
	return err
}

// pickBucket 路由起点：轮询 + 随机抖动，打散热点。
// 真正的"选有货的桶"在 Lua 里完成：以这里返回的桶为起点按序探测所有分片，
// 跳到第一个有货的桶扣减，空桶不再浪费一次请求。
func (e *Engine) pickBucket() int {
	e.mu.Lock()
	defer e.mu.Unlock()
	// 轮询 + 随机抖动，比纯随机更均匀
	b := e.nextB % e.buckets
	e.nextB++
	return b
}

// Seckill 秒杀主入口：Lua 原子完成 判重 + 扣库存 + 写流水。
// 成功后再异步落库（本地消息表 → worker 下单）。
func (e *Engine) Seckill(ctx context.Context, userID int64) (string, error) {
	orderNo := fmt.Sprintf("SK%d%d%d", e.actID, userID, time.Now().UnixNano())
	start := e.pickBucket()

	// KEYS[1] 判重；KEYS[2..] 从 start 开始轮转排列的所有分片。
	// Lua 内按序找第一个有货的桶，避免"选中空桶直接 410"的空轮询。
	keys := make([]string, 0, e.buckets+1)
	keys = append(keys, e.userKey(userID))
	for i := 0; i < e.buckets; i++ {
		keys = append(keys, e.stockKey((start+i)%e.buckets))
	}

	res, err := e.deduct.Run(ctx, e.rdb, keys,
		orderNo, userID, e.expireAt.Unix()).Int()
	if err != nil {
		return "", err
	}
	switch {
	case res == -1:
		return "", ErrDuplicate // 一人一次，资格锁死
	case res == 0:
		return "", ErrSoldOut // 所有桶都空了，真售罄
	default:
		// res 是尝试序号（1-based），换算回真实桶号
		bucket := (start + res - 1) % e.buckets
		// 扣减成功 → 写本地消息表（模拟事务消息：扣减与消息同成功）
		if err := e.writeLocalMessage(ctx, orderNo, userID, bucket); err != nil {
			// 消息写失败必须归还库存，否则少卖
			_ = e.Restore(ctx, orderNo, bucket, userID)
			return "", err
		}
		return orderNo, nil
	}
}

// writeLocalMessage 写本地消息表（下单异步化）
func (e *Engine) writeLocalMessage(ctx context.Context, orderNo string, userID int64, bucket int) error {
	// 从流水读价格（模拟：实际应查商品主数据缓存）
	price := 99900
	_, err := e.db.ExecContext(ctx,
		`INSERT INTO local_message(order_no,activity_id,sku_id,user_id,bucket_no,qty,pay_amount)
		 VALUES(?,?,?,?,?,1,?)`,
		orderNo, e.actID, e.skuID, userID, bucket, price)
	return err
}

// Restore 归还库存到【原分片】+ 释放资格 + 核销流水。
// 对应复习点：归还是冲销流水，必须回流水里记的那个桶，还任意桶总数对但账会乱。
func (e *Engine) Restore(ctx context.Context, orderNo string, bucket int, userID int64) error {
	_, err := e.restore.Run(ctx, e.rdb,
		[]string{e.stockKey(bucket), e.userKey(userID), e.flowKey(orderNo)},
		orderNo).Int()
	if err != nil {
		return err
	}
	// 注意：res=0 表示「Redis 侧已归还过」（流水已 UNLINK），但 DB 侧未必成功——
	// 上一轮可能 Lua 成功、随后 DB 事务失败（超时等），留下 status=1 的僵尸单。
	// 这里必须继续做幂等的 DB 核销，否则订单永远关不掉、对账会多算库存。
	// 落库：流水核销 + 资格释放 + 订单关单（一次事务）
	tx, err := e.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, _ = tx.ExecContext(ctx, "UPDATE inventory_log SET settled=1 WHERE order_no=? AND op_type=1", orderNo)
	_, _ = tx.ExecContext(ctx, "UPDATE user_qualification SET status=2 WHERE activity_id=? AND user_id=?", e.actID, userID)
	_, _ = tx.ExecContext(ctx, "UPDATE seckill_order SET status=3 WHERE order_no=?", orderNo)
	return tx.Commit()
}

// TotalStock 查 Redis 各分片库存之和（对账用）
func (e *Engine) TotalStock(ctx context.Context) (int, error) {
	total := 0
	for i := 0; i < e.buckets; i++ {
		v, err := e.rdb.Get(ctx, e.stockKey(i)).Int()
		if err != nil {
			return 0, err
		}
		total += v
	}
	return total, nil
}

// AllBucketStock 查每个分片的库存（不均匀性诊断用）
// 返回 key 为 "b0","b1",...,"b9" 的桶 → 库存映射。
func (e *Engine) AllBucketStock(ctx context.Context) (map[string]int, error) {
	m := make(map[string]int, e.buckets)
	for i := 0; i < e.buckets; i++ {
		v, err := e.rdb.Get(ctx, e.stockKey(i)).Int()
		if err != nil {
			return nil, err
		}
		m[fmt.Sprintf("b%d", i)] = v
	}
	return m, nil
}

// OrderCount 查已成功下单数
func (e *Engine) OrderCount(ctx context.Context) (int, error) {
	var n int
	err := e.db.QueryRowContext(ctx,
		"SELECT COUNT(*) FROM seckill_order WHERE status IN (1,2)").Scan(&n)
	return n, err
}

// PickBucketForRetry 供调用方换桶重试（库存不足时）
func (e *Engine) PickBucketForRetry() int {
	return rand.Intn(e.buckets)
}

// Cleanup 活动级清理：活动结束 + 对账/售后期后，按活动前缀批量清业务 key。
// 三种 key 都带 activity 维度，前缀匹配不会误伤其他活动：
//
//	seckill:stock:{act}:*   分片库存
//	seckill:user:{act}:*    判重资格
//	seckill:flow:SK{act}*   扣减流水（orderNo = SK + actID + uid + 时间戳）
//
// 关键：用 SCAN 游标迭代 + UNLINK 异步删除，绝不用 KEYS + DEL。
//
//	KEYS 一次性遍历全库阻塞主线程；DEL 大 key 同步释放内存也阻塞；
//	UNLINK 只把 key 摘链，内存回收丢给后台线程，主线程零阻塞。
func (e *Engine) Cleanup(ctx context.Context) (map[string]int64, error) {
	patterns := []struct {
		name string
		pat  string
	}{
		{"stock", fmt.Sprintf("seckill:stock:%d:*", e.actID)},
		{"user", fmt.Sprintf("seckill:user:%d:*", e.actID)},
		{"flow", fmt.Sprintf("seckill:flow:SK%d*", e.actID)},
	}
	deleted := make(map[string]int64, len(patterns))
	for _, p := range patterns {
		var cursor uint64
		var count int64
		for {
			keys, next, err := e.rdb.Scan(ctx, cursor, p.pat, 500).Result()
			if err != nil {
				return deleted, fmt.Errorf("scan %s: %w", p.name, err)
			}
			if len(keys) > 0 {
				n, err := e.rdb.Unlink(ctx, keys...).Result()
				if err != nil {
					return deleted, fmt.Errorf("unlink %s: %w", p.name, err)
				}
				count += n
			}
			cursor = next
			if cursor == 0 {
				break
			}
		}
		deleted[p.name] = count
	}
	return deleted, nil
}
