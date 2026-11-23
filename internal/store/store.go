package store

import (
	"context"
	"database/sql"
	"fmt"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// Store 持有 MySQL 与 Redis 两个客户端
type Store struct {
	DB    *sql.DB
	Redis *redis.Client
}

func New(mysqlDSN, redisAddr, redisPwd string) (*Store, error) {
	db, err := sql.Open("mysql", mysqlDSN)
	if err != nil {
		return nil, fmt.Errorf("mysql open: %w", err)
	}
	db.SetMaxOpenConns(50)
	db.SetMaxIdleConns(20)
	db.SetConnMaxLifetime(time.Hour)
	if err := db.Ping(); err != nil {
		return nil, fmt.Errorf("mysql ping: %w", err)
	}

	rdb := redis.NewClient(&redis.Options{
		Addr:     redisAddr,
		Password: redisPwd,
		DB:       0,
	})
	if err := rdb.Ping(context.Background()).Err(); err != nil {
		return nil, fmt.Errorf("redis ping: %w", err)
	}

	return &Store{DB: db, Redis: rdb}, nil
}

func (s *Store) Close() {
	if s.DB != nil {
		_ = s.DB.Close()
	}
	if s.Redis != nil {
		_ = s.Redis.Close()
	}
}
