package model

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"../common"
)

// AAC file syntax
type AAC struct {
	AdviceFileName      string  `json:"AdviceFileName"`
	AdviceProvider      string  `json:"AdviceProvider"`
	Version             uint32  `json:"Version"`
	AccountActivityType string  `json:"AccountActivityType"`
	DownloadedTime      string  `json:"DownloadedTime"`
	ActivityTime        string  `json:"TimeStamp"`
	MerchantID          string  `json:"MerchantId"`
	Currency            string  `json:"Currency"`
	Amount              float32 `json:"Amount"`
	CorrelationID       string  `json:"CorrelationId"`
	AdditionalData      string  `json:"AdditionalData"`
	RecordID            string  `json:"RecordId"`
}

// AccountActivity data model
type AccountActivity struct {
	AdviceFileName      string
	AdviceProvider      string
	VersionNumber       uint32
	AccountActivityType string
	Time                time.Time
	MerchantID          string
	Currency            string
	Amount              float32
	DownloadedTime      time.Time
	LastModifiedTime    time.Time
}

// AccountActivityBatch - slice of AccountActivity
type AccountActivityBatch struct {
	Batch map[uint32]AccountActivity
}

// Count - get length of map
func (batch *AccountActivityBatch) Count() int {
	return len(batch.Batch)
}

// NewAccountActivityBatch - constructor
func NewAccountActivityBatch() *AccountActivityBatch {
	var batch AccountActivityBatch
	batch.Batch = make(map[uint32]AccountActivity)
	return &batch
}

// GetHashCode - get hash code
func (act *AccountActivity) GetHashCode() uint32 {
	time := act.Time.UTC().Unix()
	s := act.MerchantID + act.AccountActivityType + strconv.FormatInt(time, 10) + act.Currency
	hash := util.Hash(s)
	return hash
}

// LoadData - converts AAC to AccountActivity
func (act *AccountActivity) LoadData(aac AAC) {
	act.AdviceFileName = aac.AdviceFileName
	act.AdviceProvider = aac.AdviceProvider
	act.VersionNumber = aac.Version
	act.AccountActivityType = aac.AccountActivityType
	act.Time = util.ParseTime(aac.ActivityTime)
	act.MerchantID = aac.MerchantID
	act.Currency = aac.Currency
	act.Amount = aac.Amount
	act.DownloadedTime = util.ParseTime(aac.DownloadedTime)
	act.LastModifiedTime = time.Now().UTC()
}

// BatchName - get file name or batch name
func (act AccountActivity) BatchName() string {
	return act.AdviceFileName
}

// ProviderName - get payment provider name
func (act AccountActivity) ProviderName() string {
	return act.AdviceProvider
}

// Version - get data version number
func (act AccountActivity) Version() uint32 {
	return act.VersionNumber
}

// ActivityTime - time of underlining activity
func (act AccountActivity) ActivityTime() time.Time {
	return act.Time
}

// CategoryID - merchant ID
func (act AccountActivity) CategoryID() string {
	return act.MerchantID
}

// DocAmount - amount in document currency
func (act AccountActivity) DocAmount() float32 {
	return act.Amount
}

// LocAmount - amount in local currency
func (act AccountActivity) LocAmount() float32 {
	// To do
	return 0
}

// GrpAmount - amount in group currency
func (act AccountActivity) GrpAmount() float32 {
	// To do
	return 0
}

// DocCurrency - document currency
func (act AccountActivity) DocCurrency() string {
	return act.Currency
}

// LocCurrency - local currency
func (act AccountActivity) LocCurrency() string {
	return act.Currency
}

// GrpCurrency - group currency
func (act AccountActivity) GrpCurrency() string {
	return "USD"
}

// ActivityType - type of underling activity
func (act AccountActivity) ActivityType() string {
	return act.AccountActivityType
}

// ProcessingTime - time of processing the activity
func (act AccountActivity) ProcessingTime() time.Time {
	return act.LastModifiedTime
}

// SetProcessingTime - set last modified time
func (act *AccountActivity) SetProcessingTime(time time.Time) {
	act.LastModifiedTime = time
}

// LoadDataFile - loads AAC file into data model
func (batch *AccountActivityBatch) LoadDataFile(filename string) (count int) {
	file, e := os.Open(filename)
	if e != nil {
		log.Fatal("File error:" + e.Error())
		os.Exit(3)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		buffer := scanner.Bytes()
		// line := scanner.Text()
		// println(string(buffer))
		var aac AAC
		json.Unmarshal(buffer, &aac)
		var activity AccountActivity
		activity.LoadData(aac)
		hash := activity.GetHashCode()
		if _, ok := batch.Batch[hash]; !ok {
			batch.Batch[hash] = activity
		}
	}

	return batch.Count()
}
