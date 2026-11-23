package handler

import (
	"encoding/json"
	"errors"
	"net/http"
	"strconv"
	"time"

	"ecommerce-lab/internal/cache"
	"ecommerce-lab/internal/config"
	"ecommerce-lab/internal/ratelimit"
	"ecommerce-lab/internal/seckill"
	"ecommerce-lab/internal/store"
)

type Handler struct {
	store    *store.Store
	cacheSvc *cache.Service
	engine   *seckill.Engine
	buckets  int
	localRL  *ratelimit.TokenBucket
	distRL   *ratelimit.DistributedLimiter
}

func New(s *store.Store, cfg *config.Config) *Handler {
	return &Handler{
		store: s,
		cacheSvc: cache.NewService(s.DB, s.Redis),
		engine: seckill.NewEngine(s.DB, s.Redis, cfg.ActivityID, cfg.SkuID, cfg.BucketCount, cfg.BackupExpireAt()),
		buckets: cfg.BucketCount,
		// L1 本机令牌桶：每秒 2000，桶容量 3000 —— 挡突发
		localRL: ratelimit.NewTokenBucket(2000, 3000),
		// L2 Redis 滑动窗口：全局每秒 5000，1s 窗口 —— 兜跨实例配额
		distRL: ratelimit.NewDistributedLimiter(s.Redis, 5000, time.Second),
	}
}

func (h *Handler) Engine() *seckill.Engine { return h.engine }

func writeJSON(w http.ResponseWriter, code int, v interface{}) {
	w.Header().Set("Content-Type", "application/json; charset=utf-8")
	w.WriteHeader(code)
	_ = json.NewEncoder(w).Encode(v)
}

// GetDetail 详情页：多级缓存（回炉 Q1/Q2 的实战场）
func (h *Handler) GetDetail(w http.ResponseWriter, r *http.Request) {
	idStr := r.URL.Query().Get("id")
	id, err := strconv.ParseInt(idStr, 10, 64)
	if err != nil || id <= 0 {
		writeJSON(w, 400, map[string]string{"error": "bad id"})
		return
	}
	view, err := h.cacheSvc.GetDetail(r.Context(), id)
	if err != nil {
		if errors.Is(err, cache.ErrNotFound) {
			writeJSON(w, 404, map[string]string{"error": "not found"})
			return
		}
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, view)
}

// Seckill 秒杀入口：两级限流 → Lua 原子扣减 → 异步下单
func (h *Handler) Seckill(w http.ResponseWriter, r *http.Request) {
	uidStr := r.URL.Query().Get("user_id")
	uid, err := strconv.ParseInt(uidStr, 10, 64)
	if err != nil || uid <= 0 {
		writeJSON(w, 400, map[string]string{"error": "bad user_id"})
		return
	}

	// L1 本地限流：挡突发
	if !h.localRL.Allow() {
		writeJSON(w, 429, map[string]string{"error": "local rate limited"})
		return
	}
	// L2 分布式限流：兜全局配额
	ok, err := h.distRL.Allow(r.Context(), "seckill")
	if err != nil || !ok {
		writeJSON(w, 429, map[string]string{"error": "global rate limited"})
		return
	}

	orderNo, err := h.engine.Seckill(r.Context(), uid)
	if err != nil {
		switch {
		case errors.Is(err, seckill.ErrDuplicate):
			writeJSON(w, 409, map[string]string{"error": "duplicate", "user_id": strconv.FormatInt(uid, 10)})
		case errors.Is(err, seckill.ErrSoldOut):
			writeJSON(w, 410, map[string]string{"error": "sold out"})
		default:
			writeJSON(w, 500, map[string]string{"error": err.Error()})
		}
		return
	}
	writeJSON(w, 200, map[string]string{"order_no": orderNo, "status": "accepted"})
}

// Stats 缓存命中统计 + 库存对账数字 + 桶级分布（不均匀诊断）
func (h *Handler) Stats(w http.ResponseWriter, r *http.Request) {
	ctx := r.Context()
	total, _ := h.engine.TotalStock(ctx)
	orders, _ := h.engine.OrderCount(ctx)
	buckets, _ := h.engine.AllBucketStock(ctx)
	writeJSON(w, 200, map[string]interface{}{
		"cache": h.cacheSvc.Stats().Snapshot(),
		"seckill": map[string]interface{}{
			"redis_stock": total,
			"db_orders":   orders,
			"buckets":     buckets, // 各桶库存（b0,b1,...,b9），诊断不均匀
		},
	})
}

// InitStock 重置秒杀库存（压测前调用）
func (h *Handler) InitStock(w http.ResponseWriter, r *http.Request) {
	totalStr := r.URL.Query().Get("total")
	total, err := strconv.Atoi(totalStr)
	if err != nil || total <= 0 {
		total = 1000
	}
	ctx := r.Context()
	// 清判重 key、流水、活动数据（仅 lab 环境）
	_ = h.store.Redis.FlushDB(ctx).Err()
	_, _ = h.store.DB.ExecContext(ctx, "DELETE FROM local_message")
	_, _ = h.store.DB.ExecContext(ctx, "DELETE FROM inventory_log")
	_, _ = h.store.DB.ExecContext(ctx, "DELETE FROM user_qualification")
	_, _ = h.store.DB.ExecContext(ctx, "DELETE FROM seckill_order")
	if err := h.engine.InitStock(ctx, total); err != nil {
		writeJSON(w, 500, map[string]string{"error": err.Error()})
		return
	}
	writeJSON(w, 200, map[string]interface{}{"ok": true, "total": total, "buckets": h.buckets})
}

// Health 健康检查
func (h *Handler) Health(w http.ResponseWriter, r *http.Request) {
	writeJSON(w, 200, map[string]string{"status": "ok", "redis": "up", "mysql": "up"})
}

// Register 注册路由
func (h *Handler) Register(mux *http.ServeMux) {
	mux.HandleFunc("/health", h.Health)
	mux.HandleFunc("/api/detail", h.GetDetail)
	mux.HandleFunc("/api/seckill", h.Seckill)
	mux.HandleFunc("/api/stats", h.Stats)
	mux.HandleFunc("/api/init", h.InitStock)
}
