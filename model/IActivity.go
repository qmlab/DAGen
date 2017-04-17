// Financial data model

package model

import (
	"time"

	mgo "gopkg.in/mgo.v2"
)

// IActivity - Data activity
type IActivity interface {
	BatchID() string // Anything used for versioning ID: could be filename or record ID
	Provider() string
	Version() uint32
	ActivityTime() time.Time
	CategoryID() string // MerchantID for most cases
	DocAmount() float32
	SetDocAmount(float32)
	LocAmount() float32
	GrpAmount() float32
	DocCurrency() string
	Type() string
	ProcessingTime() time.Time
	GetHashCode() uint32
	SetProcessingTime(time.Time)
}

// IActivityBatch - Activity batch
type IActivityBatch interface {
	LoadDataFile(file string) int
	Count() int
	Clear()
	GetKeys() (string, string, uint32)
	GetAndCompareLastBatch(string, string, uint32, uint32, *mgo.Collection, *mgo.Collection)
	InsertToStore(*mgo.Collection)
}

// IActivityOperation - operations for IActivity
type IActivityOperation interface {
	GetLastVersion(*mgo.Query) uint32
}
