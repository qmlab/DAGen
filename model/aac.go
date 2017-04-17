package model

import (
	"bufio"
	"encoding/json"
	"log"
	"os"
	"strconv"
	"time"

	"gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"../common"
)

// AAC file syntax
type AAC struct {
	AdviceFileName string  `json:"AdviceFileName"`
	AdviceProvider string  `json:"AdviceProvider"`
	Version        uint32  `json:"Version"`
	ActivityType   string  `json:"AccountActivityType"`
	DownloadedTime string  `json:"DownloadedTime"`
	ActivityTime   string  `json:"TimeStamp"`
	MerchantID     string  `json:"MerchantId"`
	Currency       string  `json:"Currency"`
	Amount         float32 `json:"Amount"`
	CorrelationID  string  `json:"CorrelationId"`
	AdditionalData string  `json:"AdditionalData"`
	RecordID       string  `json:"RecordId"`
}

// AccountActivity data model
type AccountActivity struct {
	BatchName        string
	AdviceProvider   string
	VersionNumber    uint32
	ActivityType     string
	Time             time.Time
	MerchantID       string
	Currency         string
	Amount           float32
	DownloadedTime   time.Time
	LastModifiedTime time.Time
}

// AccountActivityBatch - slice of AccountActivity
type AccountActivityBatch struct {
	Batch map[uint32]AccountActivity
}

// AccountActivityOperation - operations for AccountActivity
type AccountActivityOperation struct {
}

// GetLastVersion - get last version for the key
func (op AccountActivityOperation) GetLastVersion(query *mgo.Query) uint32 {
	var last AccountActivity
	err := query.One(&last)
	if err != nil {
		log.Fatal(err)
	}
	return last.VersionNumber
}

// InsertToStore - insert records to store
func (batch AccountActivityBatch) InsertToStore(col *mgo.Collection) {
	for _, v := range batch.Batch {
		err := col.Insert(&v)
		if err != nil {
			log.Fatal(err)
		}
	}
}

// GetAndCompareLastBatch - get and compare last batch with current batch
func (batch *AccountActivityBatch) GetAndCompareLastBatch(batchid string, provider string, version uint32, lastVer uint32, cData *mgo.Collection, cDA *mgo.Collection) {
	now := time.Now().UTC()
	var lastRecords []AccountActivity
	err := cData.Find(bson.M{"batchname": batchid, "adviceprovider": provider, "versionnumber": lastVer}).All(&lastRecords)
	if err != nil {
		log.Fatal(err)
	}

	for _, o := range lastRecords {
		hash := o.GetHashCode()
		// If record with same key exists
		if v, ok := batch.Batch[hash]; ok {
			diff := v.DocAmount() - o.DocAmount()
			if diff != 0 {
				v.SetDocAmount(diff)
				err = cDA.Insert(&v)
				if err != nil {
					log.Fatal(err)
				}
			}
			delete(batch.Batch, hash)
		} else {
			// If record has been removed
			o.SetDocAmount(-o.DocAmount())
			o.SetProcessingTime(now)
			err = cDA.Insert(&o)
			if err != nil {
				log.Fatal(err)
			}
		}
	}
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

// Clear - reset the buffer
func (batch *AccountActivityBatch) Clear() {
	batch.Batch = make(map[uint32]AccountActivity)
}

// GetKeys - get batchid, provider and version of current batch
func (batch AccountActivityBatch) GetKeys() (batchid string, provider string, version uint32) {
	for _, data := range batch.Batch {
		batchid = data.BatchID()
		version = data.Version()
		provider = data.ProviderName()
		return
	}
	return
}

// GetHashCode - get hash code
func (act *AccountActivity) GetHashCode() uint32 {
	t := act.Time.UTC().Unix()
	s := act.MerchantID + act.ActivityType + strconv.FormatInt(t, 10) + act.Currency
	hash := util.Hash(s)
	return hash
}

// LoadData - converts AAC to AccountActivity
func (act *AccountActivity) LoadData(aac AAC) {
	act.BatchName = aac.AdviceFileName
	act.AdviceProvider = aac.AdviceProvider
	act.VersionNumber = aac.Version
	act.ActivityType = aac.ActivityType
	act.Time = util.ParseTime(aac.ActivityTime)
	act.MerchantID = aac.MerchantID
	act.Currency = aac.Currency
	act.Amount = aac.Amount
	act.DownloadedTime = util.ParseTime(aac.DownloadedTime)
	act.LastModifiedTime = time.Now().UTC()
}

// BatchID - get file name or batch name
func (act AccountActivity) BatchID() string {
	return act.BatchName
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

// SetDocAmount - set amount
func (act *AccountActivity) SetDocAmount(amount float32) {
	act.Amount = amount
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

// Type - type of underling activity
func (act AccountActivity) Type() string {
	return act.ActivityType
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
