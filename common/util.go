package util

import (
	"hash/fnv"
	"log"
	"time"
)

// ParseTime - deserialize time from data
func ParseTime(str string) time.Time {
	t, e := time.Parse("2006-01-02T15:04:05.000-07:00", str)
	if e != nil {
		t, e = time.Parse("2006-01-02T15:04:05.0000000-07:00", str)
	}
	if e != nil {
		t, e = time.Parse("2006-01-02T15:04:05.0000000", str)
	}
	if e != nil {
		t, e = time.Parse("2006-01-02T15:04:05-07:00", str)
	}
	if e != nil {
		t, e = time.Parse("2006-01-02T15:04:05", str)
	}
	if e != nil {
		t, e = time.Parse("2006-01-02T15:04:05Z", str)
	}
	if e != nil {
		println("failed to parse time:", str)
		log.Fatal(e)
	}
	return t
}

// Hash - get hash for string
func Hash(s string) uint32 {
	h := fnv.New32a()
	h.Write([]byte(s))
	return h.Sum32()
}
