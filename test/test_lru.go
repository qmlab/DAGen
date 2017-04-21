package main

import (
	"sync"

	"../cache"
)

func main() {
	lru := cache.New(2)
	var wg sync.WaitGroup
	wg.Add(2)
	go func() {
		lru.Put(2, 1)
		lru.Put(2, 2)
		println(lru.Get(2).(int))
		wg.Done()
	}()
	go func() {
		lru.Put(2, 3)
		lru.Put(4, 1)
		println(lru.Get(2).(int))
		wg.Done()
	}()
	wg.Wait()
}
