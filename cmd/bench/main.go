package main

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net/http"
	"sort"
	"sync"
	"sync/atomic"
	"time"

	"github.com/redis/go-redis/v9"
)

// bench 压测脚本：纯 Go 实现，不依赖 wrk/JMeter 等外部压测工具。
//  场景1 详情页读：go run ./cmd/bench -c 100 -n 10000 -mode detail
//  场景2 秒杀写  ：go run ./cmd/bench -c 200 -n 2000 -mode seckill
//  场景3 不均匀  ：go run ./cmd/bench -mode stress
//                   需要先 PAY_TIMEOUT_S=5 启动 server（让 Worker 快速归还）
//                   bench 直连 Redis 打空指定桶（-redis-addr / -act 可调）
// 统计：QPS / P50 / P95 / P99 / 错误分类 / 桶级库存分布 / 归还前后对比

var (
	baseURL   = flag.String("url", "http://127.0.0.1:8080", "服务地址")
	conc      = flag.Int("c", 100, "并发数")
	total     = flag.Int("n", 10000, "总请求数")
	mode      = flag.String("mode", "detail", "detail | seckill | stress")
	pid       = flag.Int64("pid", 1001, "商品ID(detail 模式)")
	redisAddr = flag.String("redis-addr", "127.0.0.1:6380", "Redis 地址（stress 模式直连，用于打空指定桶）")
	redisPwd  = flag.String("redis-pwd", "", "Redis 密码")
	actID     = flag.Int64("act", 3001, "活动ID（stock key = seckill:stock:{act}:{bucket}）")
)

type result struct {
	code  int
	latMs float64
	err   string
}

func main() {
	flag.Parse()
	client := &http.Client{
		Timeout: 5 * time.Second,
		Transport: &http.Transport{
			MaxIdleConnsPerHost: *conc,
			MaxConnsPerHost:     *conc * 2,
		},
	}

	switch *mode {
	case "stress":
		rdb := redis.NewClient(&redis.Options{Addr: *redisAddr, Password: *redisPwd})
		defer rdb.Close()
		runStress(client, rdb)
	default:
		runNormal(client)
	}
}

func runNormal(client *http.Client) {
	jobs := make(chan int, *total)
	results := make(chan result, *total)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < *conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				url := fmt.Sprintf("%s/api/detail?id=%d", *baseURL, *pid)
				if *mode == "seckill" {
					uid := atomic.AddInt64(&uidSeq, 1)
					url = fmt.Sprintf("%s/api/seckill?user_id=%d", *baseURL, uid)
				}
				t0 := time.Now()
				r := doGet(client, url)
				r.latMs = float64(time.Since(t0).Microseconds()) / 1000.0
				results <- r
			}
		}()
	}
	for i := 0; i < *total; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)
	elapsed := time.Since(start)

	printSummary(*mode, *total, *conc, elapsed, collectResults(results))
	if *mode == "seckill" {
		checkOversell(client, 1000)
	}
}

// ============================================================================
// stress 模式：模拟"不支付 → 归还 → 不均匀"的完整链路
// 前提：server 用 PAY_TIMEOUT_S=5 启动（让 Worker 5 秒后触发超时关单）
// ============================================================================

// stressParams 各阶段参数（函数内常量，不暴露 flag）
const (
	stressTotal    = 1000 // 库存总量（与 server 初始值对齐）
	stressBuckets  = 10   // 桶数（与 server BUCKET_COUNT 对齐）
	stressR1Reqs   = 1000 // 第一轮：1000 个用户抢 1000 件 → 应全覆盖
	stressR2Reqs   = 1200 // 第二轮：1200 个用户，其中 8 桶有 100 件 = 800 可抢
	stressR3Reqs   = 400  // 第三轮：归还后再抢 400 个用户
	stressConc     = 200  // 并发数
	stressBadBuck0 = 3    // 要"打空"的桶（模拟异常订单永远不还）
	stressBadBuck1 = 7

	// Worker 归还吞吐受 MySQL 单事务 fsync 限制（本机实测 ~100 单/秒），
	// 1000 单要几十秒才能还完，所以不能死等固定秒数，必须轮询库存。
	stressRestoreTimeout = 120 * time.Second
)

func runStress(client *http.Client, rdb *redis.Client) {
	fmt.Println("\n===== STRESS MODE · 不均匀场景测试 =====")
	fmt.Printf("库存 %d / 桶数 %d / PAY_TIMEOUT_S 需设为 5（默认 300 要 5 分钟才关单）\n", stressTotal, stressBuckets)
	fmt.Printf("Redis %s / 活动ID %d（打空桶用）\n", *redisAddr, *actID)

	pingCtx, pingCancel := context.WithTimeout(context.Background(), 2*time.Second)
	err := rdb.Ping(pingCtx).Err()
	pingCancel()
	if err != nil {
		fmt.Printf("❌ 连不上 Redis %s: %v\n   用 -redis-addr 指定 server 实际使用的 Redis\n", *redisAddr, err)
		return
	}

	// Step 0: 重置库存
	fmt.Printf("\n▶ Step 0: InitStock 重置到 %d 桶 × %d = %d\n",
		stressBuckets, stressTotal/stressBuckets, stressTotal)
	doGet(client, fmt.Sprintf("%s/api/init?total=%d", *baseURL, stressTotal))
	time.Sleep(300 * time.Millisecond)
	printBuckets(client, "Step 0: 初始化后")

	// Step 1: 第一轮抢购（干净场景，各桶均匀）
	fmt.Printf("\n▶ Step 1: 第一轮抢购 %d 用户（库存充足，无不均匀）\n", stressR1Reqs)
	s1 := runRound(client, stressR1Reqs, stressConc)
	fmt.Printf("  200=%d  409=%d  410=%d  429=%d  500=%d\n",
		s1.codeCount[200], s1.codeCount[409], s1.codeCount[410], s1.codeCount[429], s1.codeCount[500])
	printBuckets(client, "Step 1: 第一轮抢购后（库存应接近 0 或均匀消耗）")

	// Step 2: 等 Worker 关单归还（轮询，不能死等固定秒数）
	fmt.Printf("\n▶ Step 2: 等 Worker 超时关单归还（轮询 /api/stats，最长 %s）\n", stressRestoreTimeout)
	restored, waited, ok := waitStock(client, stressTotal, stressRestoreTimeout)
	if !ok {
		fmt.Printf("\n❌ %s 内总库存没有回到 %d（当前 %d）\n",
			stressRestoreTimeout, stressTotal, sumBuckets(restored))
		fmt.Println("   ① 先确认 server 用 PAY_TIMEOUT_S=5 启动，否则默认 300s 内根本不会关单；")
		fmt.Println("   ② 若已设置，说明 Worker 归还吞吐不足（MySQL 单事务 fsync，约 100~150 单/秒），")
		fmt.Println("      1000 单需要几十秒，可调大 -restore-timeout 或优化 Worker 批量事务。")
		return
	}
	fmt.Printf("  ⏱ 归还完成，累计耗时 %.1fs\n", waited.Seconds())
	printBuckets(client, "Step 2: 归还后（应回到均匀分布）")

	// Step 3: 制造不均匀——直连 Redis 把 bucket 3 和 bucket 7 打空（模拟异常订单）
	fmt.Printf("\n▶ Step 3: 制造不均匀 — 把 bucket %d 和 bucket %d 强制设为 0\n", stressBadBuck0, stressBadBuck1)
	zctx, zcancel := context.WithTimeout(context.Background(), 2*time.Second)
	ok0 := zeroBucket(zctx, rdb, *actID, stressBadBuck0)
	ok1 := zeroBucket(zctx, rdb, *actID, stressBadBuck1)
	zcancel()
	if !ok0 || !ok1 {
		fmt.Println("  ❌ 打空桶失败，stress 场景不成立，终止")
		return
	}
	zeroed := printBuckets(client, "Step 3: 制造不均匀后（b3,b7 应为 0）")
	if zeroed == nil {
		fmt.Println("❌ 无法读取 /api/stats，终止 stress 测试")
		return
	}
	b0, b1 := fmt.Sprintf("b%d", stressBadBuck0), fmt.Sprintf("b%d", stressBadBuck1)
	if zeroed[b0] != 0 || zeroed[b1] != 0 {
		fmt.Printf("  ❌ 打空未生效（b%d=%d, b%d=%d）——检查 -act 是否与 server 的 ACTIVITY_ID 一致\n",
			stressBadBuck0, zeroed[b0], stressBadBuck1, zeroed[b1])
		return
	}

	// Step 4: 第二轮抢购（不均匀场景：2 个空桶 + 8 个满桶）
	perBucketStock := stressTotal / stressBuckets
	aliveStock := (stressBuckets - 2) * perBucketStock
	// 修复空轮询后，请求会跳过空桶去有货的桶扣：只有「总请求 > 总库存」的部分才 410
	expected410R2 := stressR2Reqs - aliveStock
	if expected410R2 < 0 {
		expected410R2 = 0
	}
	fmt.Printf("\n▶ Step 4: 第二轮抢购 %d 用户（存活 %d 桶 × %d = %d 可抢）\n",
		stressR2Reqs, stressBuckets-2, perBucketStock, aliveStock)
	s2 := runRound(client, stressR2Reqs, stressConc)
	fmt.Printf("  200=%d  409=%d  410=%d  429=%d  500=%d\n",
		s2.codeCount[200], s2.codeCount[409], s2.codeCount[410], s2.codeCount[429], s2.codeCount[500])

	// 关键指标：410 应只等于「请求数 - 总库存」，即真售罄，而不是撞空桶
	b410 := s2.codeCount[410]
	fmt.Printf("  ⚠️  410 实际 %d (%.1f%%) | 理论 %d (%.1f%%：%d 请求 - %d 库存)\n",
		b410, float64(b410)/float64(stressR2Reqs)*100,
		expected410R2, float64(expected410R2)/float64(stressR2Reqs)*100,
		stressR2Reqs, aliveStock)
	if b410 == expected410R2 {
		fmt.Println("  ✅ 空桶已被跳过：410 全部是真售罄，没有浪费在空桶上")
	}
	printBuckets(client, "Step 4: 第二轮抢购后")

	// Step 5: 等第二轮订单归还（目标：存活 8 桶重新补满 = 800）
	fmt.Printf("\n▶ Step 5: 等第二轮订单归还（轮询到存活 8 桶 = %d，最长 %s）\n", aliveStock, stressRestoreTimeout)
	restored2, waited2, ok2 := waitStock(client, aliveStock, stressRestoreTimeout)
	if !ok2 {
		fmt.Printf("  ❌ 第二轮归还未完成（当前总库存 %d，期望 %d），后续结论会失真\n",
			sumBuckets(restored2), aliveStock)
		return
	}
	fmt.Printf("  ⏱ 归还完成，累计耗时 %.1fs\n", waited2.Seconds())
	printBuckets(client, "Step 5: 第二轮归还后（b3,b7 仍应是 0，因为是强制打空的）")

	fmt.Printf("\n▶ Step 6: 第三轮抢购 %d 用户（存活 8 桶共 %d 件，空桶 b3/b7 会被自动跳过）\n",
		stressR3Reqs, aliveStock)
	s3 := runRound(client, stressR3Reqs, stressConc)
	fmt.Printf("  200=%d  409=%d  410=%d  429=%d  500=%d\n",
		s3.codeCount[200], s3.codeCount[409], s3.codeCount[410], s3.codeCount[429], s3.codeCount[500])
	printBuckets(client, "Step 6: 第三轮抢购后")

	// 总结
	fmt.Println("\n===== 不均匀场景验证总结（修复空轮询后） =====")
	fmt.Printf("第一轮 410: %d（期望 0：1000 请求 / 1000 库存）\n", s1.codeCount[410])
	fmt.Printf("第二轮 410: %d（期望 %d：1200 请求 vs %d 库存，超出部分真售罄）\n",
		s2.codeCount[410], expected410R2, aliveStock)
	fmt.Printf("第三轮 410: %d（期望 0：400 请求 < %d 库存，空桶被跳过仍能拿到货）\n",
		s3.codeCount[410], aliveStock)
	fmt.Println("")
	fmt.Println("验证点：Seckill 的 Lua 从轮询起点按序探测所有分片，跳到第一个有货的桶扣减 →")
	fmt.Println("        ① 空桶不再占用请求配额（原实现 20% 请求直接 410）；")
	fmt.Println("        ② 410 只表示『全局售罄』，不再与『撞空桶』混淆。")

	// 最终对账：b3/b7 被强制置 0，应有库存 = stressTotal - 2 桶
	checkOversell(client, stressTotal-2*perBucketStock)
}

type roundStats struct {
	latMs     []float64
	codeCount map[int]int
}

func runRound(client *http.Client, n, conc int) roundStats {
	jobs := make(chan int, n)
	results := make(chan result, n)
	var wg sync.WaitGroup

	start := time.Now()
	for i := 0; i < conc; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for range jobs {
				uid := atomic.AddInt64(&uidSeq, 1)
				url := fmt.Sprintf("%s/api/seckill?user_id=%d", *baseURL, uid)
				t0 := time.Now()
				r := doGet(client, url)
				r.latMs = float64(time.Since(t0).Microseconds()) / 1000.0
				results <- r
			}
		}()
	}
	for i := 0; i < n; i++ {
		jobs <- i
	}
	close(jobs)
	wg.Wait()
	close(results)

	elapsed := time.Since(start)
	s := collectResults(results)
	sort.Float64s(s.latMs)

	fmt.Printf("  耗时 %.2fs / QPS %.0f / P95 %.2fms\n",
		elapsed.Seconds(), float64(n)/elapsed.Seconds(), percentile(s.latMs, 0.95))
	return s
}

func collectResults(results chan result) roundStats {
	s := roundStats{codeCount: map[int]int{}}
	for r := range results {
		s.latMs = append(s.latMs, r.latMs)
		s.codeCount[r.code]++
	}
	return s
}

func printSummary(mode string, n, conc int, elapsed time.Duration, s roundStats) {
	sort.Float64s(s.latMs)
	fmt.Printf("\n===== BENCH RESULT · mode=%s =====\n", mode)
	fmt.Printf("并发 %d / 请求 %d / 耗时 %.2fs\n", conc, n, elapsed.Seconds())
	fmt.Printf("QPS   : %.0f req/s\n", float64(n)/elapsed.Seconds())
	fmt.Printf("P50   : %.2f ms\n", percentile(s.latMs, 0.50))
	fmt.Printf("P95   : %.2f ms\n", percentile(s.latMs, 0.95))
	fmt.Printf("P99   : %.2f ms\n", percentile(s.latMs, 0.99))
	fmt.Printf("Max   : %.2f ms\n", s.latMs[len(s.latMs)-1])
	fmt.Println("状态码:", s.codeCount)
}

// zeroBucket 直连 Redis 把指定桶的库存强制设为 0（模拟异常订单永远不还）。
// 注意：lab 用的是 Redis 5.0（不支持 SET ... KEEPTTL），所以先取 PTTL，SET 后再 PEXPIRE 还原，
// 保证 InitStock 挂的 EXPIREAT 兜底过期时间不被抹掉。
func zeroBucket(ctx context.Context, rdb *redis.Client, act int64, bucket int) bool {
	key := fmt.Sprintf("seckill:stock:%d:%d", act, bucket)
	pttl, err := rdb.PTTL(ctx, key).Result()
	if err != nil {
		fmt.Printf("  ❌ 读取 %s PTTL 失败: %v\n", key, err)
		return false
	}
	if err := rdb.Set(ctx, key, "0", 0).Err(); err != nil {
		fmt.Printf("  ❌ SET %s 0 失败: %v\n", key, err)
		return false
	}
	if pttl > 0 {
		if err := rdb.PExpire(ctx, key, pttl).Err(); err != nil {
			fmt.Printf("  ❌ 还原 %s 过期时间失败: %v\n", key, err)
			return false
		}
	}
	v, err := rdb.Get(ctx, key).Int()
	if err != nil || v != 0 {
		fmt.Printf("  ❌ 写入后回读 %s 失败: v=%d err=%v\n", key, v, err)
		return false
	}
	fmt.Printf("  ✓ %s = 0（保留 TTL）\n", key)
	return true
}

type statsSnapshot struct {
	RedisStock int            `json:"redis_stock"`
	DBOrders   int            `json:"db_orders"`
	Buckets    map[string]int `json:"buckets"`
}

func fetchStats(client *http.Client) (*statsSnapshot, error) {
	resp, err := client.Get(*baseURL + "/api/stats")
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()
	var st struct {
		Seckill statsSnapshot `json:"seckill"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&st); err != nil {
		return nil, err
	}
	return &st.Seckill, nil
}

// printBuckets 从 /api/stats 拉桶级库存分布并打印，返回桶映射（失败返回 nil）
func printBuckets(client *http.Client, label string) map[string]int {
	st, err := fetchStats(client)
	if err != nil {
		fmt.Printf("  stats 拉取失败: %v\n", err)
		return nil
	}

	fmt.Printf("  📊 %s | 总库存=%d  DB订单=%d | 桶分布:", label, st.RedisStock, st.DBOrders)
	// 按桶号排序打印
	keys := make([]string, 0, len(st.Buckets))
	for k := range st.Buckets {
		keys = append(keys, k)
	}
	sort.Strings(keys)
	for _, k := range keys {
		fmt.Printf(" %s=%d", k, st.Buckets[k])
	}
	fmt.Println()
	return st.Buckets
}

// waitStock 轮询 /api/stats，直到 Redis 总库存回到 target（Worker 归还完成）或超时。
// 不能死等固定秒数：归还吞吐取决于 Worker 的落库速度。
func waitStock(client *http.Client, target int, timeout time.Duration) (map[string]int, time.Duration, bool) {
	start := time.Now()
	for {
		st, err := fetchStats(client)
		if err == nil && st.RedisStock == target {
			return st.Buckets, time.Since(start), true
		}
		if time.Since(start) > timeout {
			if st != nil {
				return st.Buckets, time.Since(start), false
			}
			return nil, time.Since(start), false
		}
		time.Sleep(time.Second)
	}
}

func sumBuckets(m map[string]int) int {
	total := 0
	for _, v := range m {
		total += v
	}
	return total
}

// ---------- 基础工具 ----------

var uidSeq int64

func doGet(client *http.Client, url string) result {
	resp, err := client.Get(url)
	if err != nil {
		return result{code: 0, err: "network"}
	}
	defer resp.Body.Close()
	body, _ := io.ReadAll(resp.Body)
	r := result{code: resp.StatusCode}
	if resp.StatusCode != 200 {
		var m map[string]string
		_ = json.Unmarshal(body, &m)
		if e := m["error"]; e != "" {
			r.err = e
		}
	}
	return r
}

func percentile(sorted []float64, p float64) float64 {
	if len(sorted) == 0 {
		return 0
	}
	idx := int(float64(len(sorted)-1) * p)
	return sorted[idx]
}

func checkOversell(client *http.Client, expected int) {
	// 秒杀是异步落库：轮询到「Redis 剩余 + DB 订单 == 应有库存」再判定，
	// 否则会把"还没落库"误报成少卖（本机 Worker 吞吐约 100 单/秒）。
	start := time.Now()
	var st *statsSnapshot
	for {
		s, err := fetchStats(client)
		if err == nil {
			st = s
			if s.RedisStock+s.DBOrders == expected {
				break
			}
		}
		if time.Since(start) > stressRestoreTimeout {
			break
		}
		time.Sleep(time.Second)
	}
	if st == nil {
		fmt.Println("超卖校验失败: 无法读取 /api/stats")
		return
	}
	sum := st.RedisStock + st.DBOrders
	fmt.Printf("\n===== 超卖校验（等待 %.1fs 稳定）=====\n", time.Since(start).Seconds())
	fmt.Printf("Redis 剩余: %d | DB 订单: %d | 合计: %d (应有 %d)\n",
		st.RedisStock, st.DBOrders, sum, expected)
	if sum > expected {
		fmt.Println("❌ 超卖！")
	} else if sum < expected {
		fmt.Println("⚠️  少卖（可能有订单未完成异步落库，稍等再查）")
	} else {
		fmt.Println("✅ 零超卖零少卖")
	}
}
