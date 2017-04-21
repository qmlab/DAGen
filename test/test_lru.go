package main

import "../cache"

func main() {
	lru := cache.New(2)
	lru.Put(2, 1)
	lru.Put(2, 2)
	println(lru.Get(2).(int))
	lru.Put(1, 1)
	lru.Put(4, 1)
	println(lru.Get(2).(int))
}
