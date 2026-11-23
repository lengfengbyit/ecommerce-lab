package main

import (
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"

	_ "github.com/go-sql-driver/mysql"
	"github.com/redis/go-redis/v9"
)

// seed 造详情页缓存数据：把 1001 号商品的静态字段预热进 Redis
func main() {
	db, err := sql.Open("mysql", "lab:lab123@tcp(127.0.0.1:3306)/ecomm_lab?charset=utf8mb4&parseTime=True")
	if err != nil {
		log.Fatal(err)
	}
	defer db.Close()
	rdb := redis.NewClient(&redis.Options{Addr: "127.0.0.1:6379"})
	defer rdb.Close()
	ctx := context.Background()

	var id int64
	var title, subTitle, images, detail string
	err = db.QueryRowContext(ctx,
		"SELECT id,title,sub_title,images,detail FROM product WHERE id=1001").
		Scan(&id, &title, &subTitle, &images, &detail)
	if err != nil {
		log.Fatal(err)
	}
	var imgs []string
	_ = json.Unmarshal([]byte(images), &imgs)
	payload, _ := json.Marshal(map[string]interface{}{
		"id": id, "title": title, "sub_title": subTitle,
		"images": imgs, "detail": detail,
	})
	// 静态缓存带 10min TTL（与 service 回源一致）；不写 0 永不过期——
	// 缓存就该有 TTL 自动回收，永不过期是秒杀业务 state 才有的属性。
	_ = rdb.Set(ctx, fmt.Sprintf("static:%d", id), payload, 10*60*1e9).Err()

	// 动态字段预热
	var skuID int64
	var price, stock int
	_ = db.QueryRowContext(ctx, "SELECT id,price_cents,stock FROM sku WHERE id=2001").
		Scan(&skuID, &price, &stock)
	dyn, _ := json.Marshal(map[string]int{"sku_id": int(skuID), "price_cents": price, "stock": stock})
	_ = rdb.Set(ctx, fmt.Sprintf("dynamic:%d", skuID), dyn, 60*1e9).Err()

	fmt.Printf("seeded: static:%d + dynamic:%d (price=%d cents, stock=%d)\n", id, skuID, price, stock)
}
