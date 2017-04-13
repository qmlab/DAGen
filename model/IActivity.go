// Financial data model

package model

import "time"

type IActivity interface {
	BatchName() string // Anything used for versioning ID: could be filename or record ID
	Provider() string
	Version() uint32
	ActivityTime() time.Time
	CategoryID() string // MerchantID for most cases
	DocAmount() float32
	LocAmount() float32
	GrpAmount() float32
	DocCurrency() string
	ActivityType() string
	ProcessingTime() time.Time
}
