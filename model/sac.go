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

// SAC file syntax
type SAC struct {
	AdviceFileName          string  `json:"AdviceFileName"`
	AdviceProvider          string  `json:"AdviceProvider"`
	Version                 uint32  `json:"Version"`
	ActivityType            string  `json:"TransactionType"`
	DownloadedTime          string  `json:"DownloadedTime"`
	ActivityTime            string  `json:"TimeStamp"`
	MerchantID              string  `json:"MerchantId"`
	Currency                string  `json:"Currency"`
	Amount                  float32 `json:"Amount"`
	MerchantReferenceNumber string  `json:"MerchantReferenceNumber"`
	CorrelationID           string  `json:"CorrelationId"`
	AdditionalData          string  `json:"AdditionalData"`
	RecordID                string  `json:"RecordId"`
}

// SubmissionActivity data model
type SubmissionActivity struct {
	BatchName               string
	AdviceProvider          string
	VersionNumber           uint32
	ActivityType            string
	Time                    time.Time
	MerchantID              string
	Currency                string
	Amount                  float32
	MerchantReferenceNumber string
	DownloadedTime          time.Time
	LastModifiedTime        time.Time
	InternalMRN             string
	SellerOfRecord          string
	Partner                 string
}

// SubmissionActivityBatch - slice of SubmissionActivity
type SubmissionActivityBatch struct {
	Batch map[uint32]*SubmissionActivity
}

// SubmissionActivityOperation - operations for SubmissionActivity
type SubmissionActivityOperation struct {
}

// GetLastVersion - get last version for the key
func (op SubmissionActivityOperation) GetLastVersion(query *mgo.Query) uint32 {
	var last SubmissionActivity
	err := query.One(&last)
	if err != nil {
		log.Fatal(err)
	}
	return last.VersionNumber
}

// InsertToStore - insert records to store
func (batch SubmissionActivityBatch) InsertToStore(col *mgo.Collection) {
	for _, v := range batch.Batch {
		err := col.Insert(&v)
		if err != nil {
			log.Fatal(err)
		}
	}
}

func (batch *SubmissionActivityBatch) LoadAdditionalProperties(col *mgo.Collection) {
	for _, v := range batch.Batch {
		mrn := v.MerchantReferenceNumber
		txtype := v.ActivityType
		var tx Transaction
		err := col.Find(bson.M{"mrn": mrn, "adviceprovider": txtype}).One(&tx)
		if err == nil {
			v.SellerOfRecord = tx.SOR
			v.Partner = tx.Partner
			v.InternalMRN = tx.InternalMRN
		}
	}
}

// GetAndCompareLastBatch - get and compare last batch with current batch
func (batch *SubmissionActivityBatch) GetAndCompareLastBatch(batchid string, provider string, version uint32, lastVer uint32, cData *mgo.Collection, cDA *mgo.Collection) {
	now := time.Now().UTC()
	var lastRecords []SubmissionActivity
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
func (batch *SubmissionActivityBatch) Count() int {
	return len(batch.Batch)
}

// NewSubmissionActivityBatch - constructor
func NewSubmissionActivityBatch() *SubmissionActivityBatch {
	var batch SubmissionActivityBatch
	batch.Batch = make(map[uint32]*SubmissionActivity)
	return &batch
}

// Clear - reset the buffer
func (batch *SubmissionActivityBatch) Clear() {
	batch.Batch = make(map[uint32]*SubmissionActivity)
}

// GetKeys - get batchid, provider and version of current batch
func (batch SubmissionActivityBatch) GetKeys() (batchid string, provider string, version uint32) {
	for _, data := range batch.Batch {
		batchid = data.BatchID()
		version = data.Version()
		provider = data.ProviderName()
		return
	}
	return
}

// GetHashCode - get hash code
func (act *SubmissionActivity) GetHashCode() uint32 {
	t := act.Time.UTC().Unix()
	s := act.MerchantReferenceNumber + "-" + act.MerchantID + "-" + act.ActivityType + "-" + strconv.FormatInt(t, 10) + "-" + act.Currency + "-" + act.InternalMRN + "-" + act.SellerOfRecord + "-" + act.Partner
	hash := util.Hash(s)
	return hash
}

// LoadData - converts AAC to AccountActivity
func (act *SubmissionActivity) LoadData(sac SAC) {
	act.BatchName = sac.AdviceFileName
	act.AdviceProvider = sac.AdviceProvider
	act.VersionNumber = sac.Version
	act.ActivityType = sac.ActivityType
	act.Time = util.ParseTime(sac.ActivityTime)
	act.MerchantID = sac.MerchantID
	act.Currency = sac.Currency
	act.Amount = sac.Amount
	act.MerchantReferenceNumber = sac.MerchantReferenceNumber
	act.DownloadedTime = util.ParseTime(sac.DownloadedTime)
	act.LastModifiedTime = time.Now().UTC()
}

// BatchID - get file name or batch name
func (act SubmissionActivity) BatchID() string {
	return act.BatchName
}

// ProviderName - get payment provider name
func (act SubmissionActivity) ProviderName() string {
	return act.AdviceProvider
}

// Version - get data version number
func (act SubmissionActivity) Version() uint32 {
	return act.VersionNumber
}

// ActivityTime - time of underlining activity
func (act SubmissionActivity) ActivityTime() time.Time {
	return act.Time
}

// CategoryID - merchant ID
func (act SubmissionActivity) CategoryID() string {
	return act.MerchantID
}

// DocAmount - amount in document currency
func (act SubmissionActivity) DocAmount() float32 {
	return act.Amount
}

// SetDocAmount - set amount
func (act *SubmissionActivity) SetDocAmount(amount float32) {
	act.Amount = amount
}

// LocAmount - amount in local currency
func (act SubmissionActivity) LocAmount() float32 {
	// To do
	return 0
}

// GrpAmount - amount in group currency
func (act SubmissionActivity) GrpAmount() float32 {
	// To do
	return 0
}

// DocCurrency - document currency
func (act SubmissionActivity) DocCurrency() string {
	return act.Currency
}

// LocCurrency - local currency
func (act SubmissionActivity) LocCurrency() string {
	return act.Currency
}

// GrpCurrency - group currency
func (act SubmissionActivity) GrpCurrency() string {
	return "USD"
}

// Type - type of underling activity
func (act SubmissionActivity) Type() string {
	return act.ActivityType
}

// ProcessingTime - time of processing the activity
func (act SubmissionActivity) ProcessingTime() time.Time {
	return act.LastModifiedTime
}

// SetProcessingTime - set last modified time
func (act *SubmissionActivity) SetProcessingTime(time time.Time) {
	act.LastModifiedTime = time
}

// LoadDataFile - loads AAC file into data model
func (batch *SubmissionActivityBatch) LoadDataFile(filename string) (count int) {
	file, e := os.Open(filename)
	if e != nil {
		log.Fatal("File error:" + e.Error())
		os.Exit(3)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	for scanner.Scan() {
		buffer := scanner.Bytes()
		var sac SAC
		json.Unmarshal(buffer, &sac)
		var activity SubmissionActivity
		activity.LoadData(sac)
		hash := activity.GetHashCode()
		if _, ok := batch.Batch[hash]; !ok {
			batch.Batch[hash] = &activity
		}
	}

	return batch.Count()
}
