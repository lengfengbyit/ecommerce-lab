package cache

import (
	"context"
	"database/sql"
	"encoding/json"
	"errors"
	"fmt"
	"math/rand"
	"sync"
	"time"

	"github.com/redis/go-redis/v9"
)

var (
	ErrNotFound = errors.New("商品不存在") // 穿透闸门：库里真没有
)

// ProductStatic 静态字段（可长 TTL 缓存）
type ProductStatic struct {
	ID       int64    `json:"id"`
	Title    string   `json:"title"`
	SubTitle string   `json:"sub_title"`
	Images   []string `json:"images"`
	Detail   string   `json:"detail"`
}

// ProductDynamic 动态字段（价格/库存，绝不进本地缓存）
type ProductDynamic struct {
	SkuID      int64 `json:"sku_id"`
	PriceCents int   `json:"price_cents"`
	Stock      int   `json:"stock"`
}

// DetailView 详情页聚合视图：静态 + 动态分离返回
type DetailView struct {
	Static  ProductStatic  `json:"static"`
	Dynamic ProductDynamic `json:"dynamic"`
	From    string         `json:"from"` // local / redis / db / degraded
}

// Service 多级缓存服务
type Service struct {
	db      *sql.DB
	rdb     *redis.Client
	local   *LocalCache
	bloom   *BloomFilter
	sf      *singleflight // 防击穿：同 key 只有一个请求回源
	stats   *Stats
	baseTTL time.Duration
}

type Stats struct {
	mu            sync.Mutex
	LocalHits     int64
	RedisHits     int64
	DBHits        int64
	BloomRejects  int64
	Degrades      int64
	NilHits       int64 // 空值缓存命中（穿透防护）
}

func (s *Stats) Snapshot() map[string]int64 {
	s.mu.Lock()
	defer s.mu.Unlock()
	return map[string]int64{
		"local_hits": s.LocalHits, "redis_hits": s.RedisHits,
		"db_hits": s.DBHits, "bloom_rejects": s.BloomRejects,
		"degrades": s.Degrades, "nil_hits": s.NilHits,
	}
}

func (b *Stats) inc(p *int64) { b.mu.Lock(); *p = *p + 1; b.mu.Unlock() }

func NewService(db *sql.DB, rdb *redis.Client) *Service {
	bf := NewBloomFilter(1<<20, 3)
	// 预热布隆：把现有商品 ID 灌进去（生产环境应订阅 binlog 增量维护）
	rows, err := db.Query("SELECT id FROM product")
	if err == nil {
		defer rows.Close()
		for rows.Next() {
			var id int64
			if rows.Scan(&id) == nil {
				bf.Add(fmt.Sprintf("static:%d", id))
			}
		}
	}
	return &Service{
		db: db, rdb: rdb, local: NewLocalCache(1000), bloom: bf,
		sf: newSingleflight(), stats: &Stats{}, baseTTL: 10 * time.Minute,
	}
}

func (s *Service) Stats() *Stats { return s.stats }

// GetDetail 详情页主入口：静态走多级缓存，动态直读 Redis 极短 TTL。
func (s *Service) GetDetail(ctx context.Context, productID int64) (*DetailView, error) {
	key := fmt.Sprintf("static:%d", productID)

	// ① 穿透闸门：布隆说不存在 → 直接拒，不打缓存也不打 DB
	if !s.bloom.MightContain(key) {
		s.stats.inc(&s.stats.BloomRejects)
		return nil, ErrNotFound
	}

	// ② 静态字段：本地缓存 → Redis → DB（三级）
	static, from, err := s.getStatic(ctx, key, productID)
	if err != nil {
		return nil, err
	}

	// ③ 动态字段：独立接口直读 Redis，极短 TTL（价格不能错）
	dynamic, err := s.getDynamic(ctx, productID)
	if err != nil {
		// 降级：动态字段拿不到时返兜底价 0 + 标记，绝不返回错价
		s.stats.inc(&s.stats.Degrades)
		dynamic = &ProductDynamic{SkuID: 0, PriceCents: 0, Stock: -1}
		from = "degraded"
	}

	return &DetailView{Static: *static, Dynamic: *dynamic, From: from}, nil
}

func (s *Service) getStatic(ctx context.Context, key string, productID int64) (*ProductStatic, string, error) {
	// L1 本地缓存
	if v, ok := s.local.Get(key); ok {
		s.stats.inc(&s.stats.LocalHits)
		var p ProductStatic
		if json.Unmarshal(v, &p) == nil {
			return &p, "local", nil
		}
	}

	// L2 Redis：用 singleflight 防击穿（热点 key 过期时只有一个请求回源）
	val, err, _ := s.sf.Do(key, func() (interface{}, error) {
		raw, err := s.rdb.Get(ctx, key).Bytes()
		if err == redis.Nil {
			// 回源 DB
			s.stats.inc(&s.stats.DBHits)
			p, err := s.loadStaticFromDB(ctx, productID)
			if err != nil {
				return nil, err
			}
			b, _ := json.Marshal(p)
			// 雪崩防护：TTL 加随机抖动（baseTTL ± 20%）
			ttl := s.jitterTTL()
			_ = s.rdb.Set(ctx, key, b, ttl).Err()
			s.local.Set(key, b, ttl) // 回填 L1
			return b, nil
		}
		if err != nil {
			return nil, err // Redis 真挂了，走降级
		}
		// 空值缓存命中（穿透第二道闸）
		if string(raw) == "NULL" {
			s.stats.inc(&s.stats.NilHits)
			return nil, ErrNotFound
		}
		s.stats.inc(&s.stats.RedisHits)
		// 回填 L1
		s.local.Set(key, raw, s.jitterTTL())
		return raw, nil
	})
	if err != nil {
		// 降级：Redis 挂了但 L1 有过旧值？这里简单返回错误由上层标记
		return nil, "", err
	}
	raw := val.([]byte)
	var p ProductStatic
	if err := json.Unmarshal(raw, &p); err != nil {
		return nil, "", err
	}
	return &p, "redis", nil
}

func (s *Service) loadStaticFromDB(ctx context.Context, productID int64) (*ProductStatic, error) {
	var p ProductStatic
	var images string
	err := s.db.QueryRowContext(ctx,
		"SELECT id,title,sub_title,images,detail FROM product WHERE id=?", productID).
		Scan(&p.ID, &p.Title, &p.SubTitle, &images, &p.Detail)
	if err != nil {
		if errors.Is(err, sql.ErrNoRows) {
			// 空值缓存：库里真没有，缓存 NULL 短时间，防反复打 DB
			_ = s.rdb.Set(ctx, fmt.Sprintf("static:%d", productID), "NULL", 30*time.Second).Err()
			return nil, ErrNotFound
		}
		return nil, err
	}
	_ = json.Unmarshal([]byte(images), &p.Images)
	return &p, nil
}

func (s *Service) getDynamic(ctx context.Context, productID int64) (*ProductDynamic, error) {
	// 按 product_id 查（一个商品取主 SKU），缓存 key 用 product_id 保持一致
	key := fmt.Sprintf("dynamic:%d", productID)
	raw, err := s.rdb.Get(ctx, key).Bytes()
	if err == redis.Nil {
		// 回源 DB（动态字段 TTL 极短，60s）
		var d ProductDynamic
		err := s.db.QueryRowContext(ctx,
			"SELECT id,price_cents,stock FROM sku WHERE product_id=? LIMIT 1", productID).
			Scan(&d.SkuID, &d.PriceCents, &d.Stock)
		if err != nil {
			return nil, err
		}
		b, _ := json.Marshal(d)
		_ = s.rdb.Set(ctx, key, b, 60*time.Second).Err()
		return &d, nil
	}
	if err != nil {
		return nil, err
	}
	var d ProductDynamic
	if err := json.Unmarshal(raw, &d); err != nil {
		return nil, err
	}
	return &d, nil
}

// jitterTTL 雪崩防护：基础 TTL ± 20% 随机抖动，避免大批量 key 同时过期
func (s *Service) jitterTTL() time.Duration {
	base := s.baseTTL.Seconds()
	jitter := (rand.Float64()*0.4 - 0.2) * base
	return time.Duration((base + jitter) * float64(time.Second))
}

// singleflight 防击穿：相同 key 的并发请求合并为一个回源
type singleflight struct {
	mu sync.Mutex
	m  map[string]*call
}
type call struct {
	wg  sync.WaitGroup
	val interface{}
	err error
}

func newSingleflight() *singleflight { return &singleflight{m: make(map[string]*call)} }

func (g *singleflight) Do(key string, fn func() (interface{}, error)) (interface{}, error, bool) {
	g.mu.Lock()
	if c, ok := g.m[key]; ok {
		g.mu.Unlock()
		c.wg.Wait()
		return c.val, c.err, true
	}
	c := &call{}
	c.wg.Add(1)
	g.m[key] = c
	g.mu.Unlock()

	c.val, c.err = fn()
	c.wg.Done()

	g.mu.Lock()
	delete(g.m, key)
	g.mu.Unlock()
	return c.val, c.err, false
}
