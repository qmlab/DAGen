package cache

import "container/list"

// LRUCache - least recent used cache
type LRUCache struct {
	items *list.List
	table map[uint32]*list.Element
	cap   int
	count int
}

// KVP - key value pair
type KVP struct {
	key   uint32
	value interface{}
}

// New - Create a new LRU cache
func New(capacity int) LRUCache {
	var cache LRUCache
	cache.items = list.New()
	cache.table = make(map[uint32]*list.Element)
	cache.cap = capacity
	return cache
}

// Get - get a value from the cache
func (cache *LRUCache) Get(key uint32) interface{} {
	if el, ok := cache.table[key]; ok {
		v := el.Value.(KVP).value
		if el != cache.items.Back() {
			cache.items.Remove(el)
			el2 := cache.items.PushBack(KVP{key: key, value: v})
			cache.table[key] = el2
		}
		return el.Value.(KVP).value
	}
	return -1
}

// Put - ppsert a value in the cache
func (cache *LRUCache) Put(key uint32, value interface{}) {
	if el, ok := cache.table[key]; ok {
		cache.items.Remove(el)
		el2 := cache.items.PushBack(KVP{key: key, value: value})
		cache.table[key] = el2
	} else if cache.count <= cache.cap {
		if cache.count == cache.cap {
			first := cache.items.Front()
			kvp := first.Value.(KVP)
			delete(cache.table, kvp.key)
			cache.items.Remove(first)
			cache.count--
		}
		el2 := cache.items.PushBack(KVP{key: key, value: value})
		cache.table[key] = el2
		cache.count++
	}
}
