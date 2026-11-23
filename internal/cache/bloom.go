package cache

import (
	"hash/fnv"
	"sync"
)

// bloomGoldenRatio64 是 2^64 / φ（黄金比例 φ ≈ 1.618...）的整数近似值。
// 用途：布隆过滤器双哈希优化——用一个 FNV64 哈希派生 h1，再用本常量 XOR 得到 h2，
// 随后通过 g_i = h1 + i*h2 (mod m) 模拟 k 个独立哈希函数。
// 来源：Kirsch & Mitzenmacher 2008, 《Less Hashing, Same Performance: Building a Better Bloom Filter》。
const bloomGoldenRatio64 = 0x9e3779b97f4a7c15

// BloomFilter 简易布隆过滤器：防缓存穿透第一道闸。
// 语义：不存在 → 一定不存在（直接拒）；存在 → 可能存在（继续查缓存/DB）。
type BloomFilter struct {
	mu   sync.RWMutex
	bits []uint64
	m    uint64 // bit 总数
	k    uint64 // hash 次数
}

func NewBloomFilter(size uint64, hashes uint64) *BloomFilter {
	words := (size + 63) / 64 // 向上取整
	if words == 0 {
		words = 1 // 至少一个 bit
	}
	return &BloomFilter{bits: make([]uint64, words), m: size, k: hashes}
}

func (b *BloomFilter) hashes(s string) [2]uint64 {
	h := fnv.New64a()
	_, _ = h.Write([]byte(s))
	h1 := h.Sum64()
	h2 := h1 ^ bloomGoldenRatio64
	return [2]uint64{h1, h2}
}

func (b *BloomFilter) Add(s string) {
	a := b.hashes(s)
	b.mu.Lock()
	defer b.mu.Unlock()
	for i := uint64(0); i < b.k; i++ {
		pos := (a[0] + i*a[1]) % b.m
		b.bits[pos/64] |= 1 << (pos % 64)
	}
}

func (b *BloomFilter) MightContain(s string) bool {
	a := b.hashes(s)
	b.mu.RLock()
	defer b.mu.RUnlock()
	for i := uint64(0); i < b.k; i++ {
		pos := (a[0] + i*a[1]) % b.m
		if b.bits[pos/64]&(1<<(pos%64)) == 0 {
			return false
		}
	}
	return true
}
