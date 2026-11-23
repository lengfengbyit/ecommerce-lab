package seckill

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"time"
)

// Worker 后台任务：
//  1. 扫本地消息表 → 异步落订单（模拟事务消息消费）
//  2. 扫超时未支付订单 → 关单 + 归还库存到原桶 + 释放资格
//     对应复习点：事件驱动即时还，不等满时钟批量扫（缩短回血窗口）
type Worker struct {
	db         *sql.DB
	engine     *Engine
	stop       chan struct{}
	payTimeout time.Duration // 支付超时：超过这个时间没付就关单归还
}

func NewWorker(db *sql.DB, engine *Engine, payTimeout time.Duration) *Worker {
	return &Worker{db: db, engine: engine, stop: make(chan struct{}), payTimeout: payTimeout}
}

func (w *Worker) Start() {
	go w.loopMessages()
	go w.loopTimeoutOrders()
	log.Println("[worker] started: message consumer + timeout closer")
}

func (w *Worker) Stop() { close(w.stop) }

// loopMessages 扫本地消息表：把扣减成功但未下单的消息落成订单
func (w *Worker) loopMessages() {
	ticker := time.NewTicker(50 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			// 一批最多 200 条、每条一个事务；2s 在慢盘上会在批次尾部超时，
			// 导致「Lua 已归还、DB 未落」的僵尸单，给足 10s。
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			w.processMessages(ctx)
			cancel()
		}
	}
}

func (w *Worker) processMessages(ctx context.Context) {
	rows, err := w.db.QueryContext(ctx,
		`SELECT id,order_no,activity_id,sku_id,user_id,bucket_no,qty,pay_amount
		 FROM local_message WHERE status=0 ORDER BY id LIMIT 200`)
	if err != nil {
		return
	}
	defer rows.Close()
	type msg struct {
		id, act, sku, uid, bucket, qty, amount int64
		orderNo                                string
	}
	var batch []msg
	for rows.Next() {
		var m msg
		if rows.Scan(&m.id, &m.orderNo, &m.act, &m.sku, &m.uid, &m.bucket, &m.qty, &m.amount) == nil {
			batch = append(batch, m)
		}
	}
	rows.Close()

	for _, m := range batch {
		tx, err := w.db.BeginTx(ctx, nil)
		if err != nil {
			continue
		}
		// 下单：状态 1 待支付；唯一键 uk_activity_user 兜底防重复
		_, err = tx.ExecContext(ctx,
			`INSERT INTO seckill_order(order_no,activity_id,sku_id,user_id,qty,pay_amount,bucket_no,status)
			 VALUES(?,?,?,?,?,?,?,1)
			 ON DUPLICATE KEY UPDATE order_no=order_no`,
			m.orderNo, m.act, m.sku, m.uid, m.qty, m.amount, m.bucket)
		if err == nil {
			// 流水落库：记 bucket，供对账/归还依据
			_, err = tx.ExecContext(ctx,
				`INSERT INTO inventory_log(activity_id,user_id,order_no,bucket_no,qty,op_type)
				 VALUES(?,?,?,?,?,1)
				 ON DUPLICATE KEY UPDATE id=id`,
				m.act, m.uid, m.orderNo, m.bucket, m.qty)
		}
		if err == nil {
			// 资格落库兜底
			_, err = tx.ExecContext(ctx,
				`INSERT INTO user_qualification(activity_id,user_id,status,order_no)
				 VALUES(?,?,1,?)
				 ON DUPLICATE KEY UPDATE status=1`,
				m.act, m.uid, m.orderNo)
		}
		if err == nil {
			_, err = tx.ExecContext(ctx, "UPDATE local_message SET status=1 WHERE id=?", m.id)
		}
		if err != nil {
			_ = tx.Rollback()
			_, _ = w.db.ExecContext(ctx, "UPDATE local_message SET retry_count=retry_count+1 WHERE id=?", m.id)
		} else {
			_ = tx.Commit()
		}
	}
}

// loopTimeoutOrders 超时关单：把支付超时的订单关掉并归还库存
func (w *Worker) loopTimeoutOrders() {
	ticker := time.NewTicker(200 * time.Millisecond)
	defer ticker.Stop()
	for {
		select {
		case <-w.stop:
			return
		case <-ticker.C:
			// 同理：一批最多 100 单，每单 Lua + 3 条 UPDATE，2s 不够用。
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			w.closeTimeoutOrders(ctx)
			cancel()
		}
	}
}

func (w *Worker) closeTimeoutOrders(ctx context.Context) {
	// 查超时待支付订单：支付时限由 payTimeout 决定（生产默认 300s，压测可设 5s）
	// 注意：MySQL 不支持 INTERVAL ? SECOND 参数化，必须用 fmt.Sprintf 拼数字。
	// 安全：payTimeoutSeconds 来自 time.Duration.Seconds()，一定是整数，无注入风险。
	timeoutSec := int(w.payTimeout.Seconds())
	rows, err := w.db.QueryContext(ctx, fmt.Sprintf(
		`SELECT order_no,user_id,bucket_no FROM seckill_order
		 WHERE status=1 AND created_at < DATE_SUB(NOW(), INTERVAL %d SECOND)
		 ORDER BY created_at LIMIT 100`, timeoutSec))
	if err != nil {
		log.Printf("[worker] query timeout orders failed: %v", err)
		return
	}
	type order struct {
		orderNo string
		uid     int64
		bucket  int
	}
	var list []order
	for rows.Next() {
		var o order
		if rows.Scan(&o.orderNo, &o.uid, &o.bucket) == nil {
			list = append(list, o)
		}
	}
	rows.Close()

	for _, o := range list {
		// 归还到流水记录的【原桶】，释放资格，关单
		if err := w.engine.Restore(ctx, o.orderNo, o.bucket, o.uid); err != nil {
			log.Printf("[worker] restore %s failed: %v", o.orderNo, err)
		}
	}
}
