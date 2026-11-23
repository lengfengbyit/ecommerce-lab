package cache

import (
	"container/list"
	"sync"
	"time"
)

// LocalCache 本地缓存（Caffeine 风格）：TTL + 容量上限 + LRU 淘汰。
// 对应复习点：本地缓存放【静态字段】，动态字段（价格/库存）绝不进本地缓存，
// 否则改价后其余 999 个实例仍旧值，一致性兜不住。
type LocalCache struct {
	mu       sync.Mutex
	capacity int
	items    map[string]*list.Element
	lru      *list.List
}

type entry struct {
	key       string
	value     []byte
	expiresAt time.Time
}

func NewLocalCache(capacity int) *LocalCache {
	return &LocalCache{
		capacity: capacity,
		items:    make(map[string]*list.Element),
		lru:      list.New(),
	}
}

func (c *LocalCache) Get(key string) ([]byte, bool) {
	c.mu.Lock()
	defer c.mu.Unlock()
	el, ok := c.items[key]
	if !ok {
		return nil, false
	}
	e := el.Value.(*entry)
	if time.Now().After(e.expiresAt) {
		c.lru.Remove(el)
		delete(c.items, key)
		return nil, false
	}
	c.lru.MoveToFront(el)
	return e.value, true
}

func (c *LocalCache) Set(key string, value []byte, ttl time.Duration) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		e := el.Value.(*entry)
		e.value = value
		e.expiresAt = time.Now().Add(ttl)
		c.lru.MoveToFront(el)
		return
	}
	el := c.lru.PushFront(&entry{key: key, value: value, expiresAt: time.Now().Add(ttl)})
	c.items[key] = el
	// 超容量淘汰最久未用
	for c.lru.Len() > c.capacity {
		tail := c.lru.Back()
		if tail == nil {
			break
		}
		c.lru.Remove(tail)
		delete(c.items, tail.Value.(*entry).key)
	}
}

func (c *LocalCache) Delete(key string) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if el, ok := c.items[key]; ok {
		c.lru.Remove(el)
		delete(c.items, key)
	}
}

func (c *LocalCache) Len() int {
	c.mu.Lock()
	defer c.mu.Unlock()
	return c.lru.Len()
}
