package main

import (
	"context"
	"fmt"
	"log"
	"os"

	"ecommerce-lab/internal/config"
	"ecommerce-lab/internal/seckill"
	"ecommerce-lab/internal/store"
)

// cleanup 活动级清理命令：活动结束 + 对账/售后期后手动或定时任务调用。
// 用法：go run ./cmd/cleanup
// 可选环境变量 ACTIVITY_END_AT 只影响兜底 EXPIREAT 的计算，不影响清理范围——
// 清理范围严格按 activity_id 前缀，只清当前活动，不误伤别的活动。
func main() {
	cfg := config.Load()
	s, err := store.New(cfg.MySQLDSN, cfg.RedisAddr, cfg.RedisPwd)
	if err != nil {
		log.Fatalf("store init: %v", err)
	}
	defer s.Close()

	engine := seckill.NewEngine(s.DB, s.Redis, cfg.ActivityID, cfg.SkuID, cfg.BucketCount, cfg.BackupExpireAt())
	deleted, err := engine.Cleanup(context.Background())
	if err != nil {
		log.Fatalf("cleanup failed: %v", err)
	}

	fmt.Printf("activity %d cleanup done (SCAN + UNLINK):\n", cfg.ActivityID)
	for _, k := range []string{"stock", "user", "flow"} {
		fmt.Printf("  %-6s keys deleted: %d\n", k, deleted[k])
	}

	// 幂等：再跑一次应该全是 0
	again, err := engine.Cleanup(context.Background())
	if err != nil {
		log.Fatalf("verify cleanup failed: %v", err)
	}
	fmt.Printf("verify (should all be 0): stock=%d user=%d flow=%d\n",
		again["stock"], again["user"], again["flow"])
	if again["stock"]+again["user"]+again["flow"] != 0 {
		os.Exit(1)
	}
}
