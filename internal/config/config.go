package config

import (
	"os"
	"strconv"
	"time"
)

// Config 全局配置，全部支持环境变量覆盖，方便本地/容器切换
type Config struct {
	MySQLDSN     string
	RedisAddr    string
	RedisPwd     string
	ListenAddr   string
	ActivityID   int64
	SkuID        int64
	BucketCount  int
	PayTimeoutS  int
	ActivityEndAt string // RFC3339，活动结束时间；留空则默认 24h 后
	BackupTTLDays int    // 兜底 TTL：活动结束后多少天清空业务 key
}

func getenv(key, def string) string {
	if v := os.Getenv(key); v != "" {
		return v
	}
	return def
}

// atoiEnv 读取整数环境变量，不存在或非法时返回 def
func atoiEnv(key string, def int) int {
	if v := os.Getenv(key); v != "" {
		if n, err := strconv.Atoi(v); err == nil {
			return n
		}
	}
	return def
}

func Load() *Config {
	return &Config{
		// 项目自带 compose 实例：MySQL 3307 / Redis 6380（与本机 dnmp 的 3306/6379 错开）
		// 用环境变量 MYSQL_DSN / REDIS_ADDR 可随时切回本机已有服务
		MySQLDSN:    getenv("MYSQL_DSN", "lab:lab123@tcp(127.0.0.1:3307)/ecomm_lab?charset=utf8mb4&parseTime=True&loc=Local"),
		RedisAddr:   getenv("REDIS_ADDR", "127.0.0.1:6380"),
		RedisPwd:    getenv("REDIS_PWD", ""),
		ListenAddr:  getenv("LISTEN_ADDR", ":8080"),
		ActivityID:  3001,
		SkuID:       2001,
		BucketCount: 10,
		PayTimeoutS: atoiEnv("PAY_TIMEOUT_S", 300),
		ActivityEndAt: getenv("ACTIVITY_END_AT", ""),
		BackupTTLDays: 30,
	}
}

// PayTimeout 支付超时时间：Worker 用这个时长判断订单是否该关单归还库存。
// 压测时可以通过 PAY_TIMEOUT_S 环境变量设成 5~10 秒快速触发归还逻辑。
func (c *Config) PayTimeout() time.Duration {
	if c.PayTimeoutS <= 0 {
		return 300 * time.Second
	}
	return time.Duration(c.PayTimeoutS) * time.Second
}

// BackupExpireAt 兜底绝对过期时间 = 活动结束 + N 天。
// 业务 key 不设短 TTL（到点删=超卖/重复抢），但挂一个足够长的 EXPIREAT，
// 这样即使活动级清理任务挂了，Redis 也会自己回收，不会永久泄漏。
func (c *Config) BackupExpireAt() time.Time {
	end := time.Now().Add(24 * time.Hour)
	if c.ActivityEndAt != "" {
		if t, err := time.Parse(time.RFC3339, c.ActivityEndAt); err == nil {
			end = t
		}
	}
	days := c.BackupTTLDays
	if days <= 0 {
		days = 30
	}
	return end.Add(time.Duration(days) * 24 * time.Hour)
}
