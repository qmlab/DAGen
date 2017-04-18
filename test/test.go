package main

import (
	"fmt"
	"hash/fnv"
)

func hash(s string, a []int) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	a[0] = 10
	return h.Sum32()
}

func main() {
	var arr = []int{1}
	fmt.Println(hash("HelloWorld", arr))
	println(arr[0])
}
