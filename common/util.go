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

// TransactionTypeToID - trasaction type to id translation
func TransactionTypeToID(s string) uint16 {
	switch s {
	case "Charge":
		return uint16(0)
	case "Refund":
		return uint16(1)
	case "Chargeback":
		return uint16(2)
	case "ReverseChargeback":
		return uint16(3)
	case "Credit":
		return uint16(4)
	default:
		return uint16(99)
	}
}

// TransactionTypeIDToStr - transaction type id to string translation
func TransactionTypeIDToStr(id uint16) string {
	switch id {
	case 0:
		return "Charge"
	case 1:
		return "Refund"
	case 2:
		return "Chargeback"
	case 3:
		return "ReverseChargeback"
	case 4:
		return "Credit"
	default:
		return "Unknown"
	}
}
