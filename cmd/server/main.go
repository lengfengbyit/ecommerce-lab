package main

import (
	"context"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"ecommerce-lab/internal/config"
	"ecommerce-lab/internal/handler"
	"ecommerce-lab/internal/seckill"
	"ecommerce-lab/internal/store"
)

func main() {
	cfg := config.Load()
	s, err := store.New(cfg.MySQLDSN, cfg.RedisAddr, cfg.RedisPwd)
	if err != nil {
		log.Fatalf("store init: %v", err)
	}
	defer s.Close()

	h := handler.New(s, cfg)

	// 初始化分片库存（默认 1000 件）
	if err := h.Engine().InitStock(context.Background(), 1000); err != nil {
		log.Fatalf("init stock: %v", err)
	}
	// 启动异步 worker：下单 + 超时关单归还
	w := seckill.NewWorker(s.DB, h.Engine(), cfg.PayTimeout())
	w.Start()
	defer w.Stop()

	mux := http.NewServeMux()
	h.Register(mux)

	srv := &http.Server{
		Addr:              cfg.ListenAddr,
		Handler:           mux,
		ReadHeaderTimeout: 5 * time.Second,
	}

	go func() {
		log.Printf("server listening on %s", cfg.ListenAddr)
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("serve: %v", err)
		}
	}()

	// 优雅退出
	quit := make(chan os.Signal, 1)
	signal.Notify(quit, syscall.SIGINT, syscall.SIGTERM)
	<-quit
	log.Println("shutting down...")
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_ = srv.Shutdown(ctx)
}
