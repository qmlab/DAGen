package model

import (
	"bufio"
	"container/list"
	"encoding/json"
	"log"
	"os"
	"time"
)

type AAC struct {
	AdviceFileName      string    `json:"AdviceFileName"`
	AdviceProvider      string    `json:"AdviceProvider"`
	Version             uint32    `json:"Version"`
	AccountActivityType string    `json:"AccountActivityType"`
	DownloadedTime      time.Time `json:"DownloadedTime"`
	ActivityTime        time.Time `json:"TimeStamp"`
	MerchantID          string    `json:"MerchantId"`
	Currency            string    `json:"Currency"`
	Amount              float32   `json:"Amount"`
	CorrelationId       string    `json:"CorrelationId"`
	AdditionalData      string    `json:"AdditionalData"`
}

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

type AccountActivityBatch struct {
	Batch []AccountActivity
	Count int
}

func (act *AccountActivity) LoadData(aac AAC) {
	act.AdviceFileName = aac.AdviceFileName
	act.AdviceProvider = aac.AdviceProvider
	act.VersionNumber = aac.Version
	act.AccountActivityType = aac.AccountActivityType
	act.Time = aac.ActivityTime
	act.MerchantID = aac.MerchantID
	act.Currency = aac.Currency
	act.Amount = aac.Amount
	act.DownloadedTime = aac.DownloadedTime
	act.LastModifiedTime = time.Now().UTC()
}

func (act AccountActivity) BatchName() string {
	return act.AdviceFileName
}

func (act AccountActivity) ProviderName() string {
	return act.AdviceProvider
}

func (act AccountActivity) Version() uint32 {
	return act.VersionNumber
}

func (act AccountActivity) ActivityTime() time.Time {
	return act.Time
}

func (act AccountActivity) CategoryID() string {
	return act.MerchantID
}

func (act AccountActivity) DocAmount() float32 {
	return act.Amount
}

func (act AccountActivity) LocAmount() float32 {
	// To do
	return 0
}

func (act AccountActivity) GrpAmount() float32 {
	// To do
	return 0
}

func (act AccountActivity) DocCurrency() string {
	return act.Currency
}

func (act AccountActivity) ActivityType() string {
	return act.AccountActivityType
}

func (act AccountActivity) ProcessingTime() time.Time {
	return act.LastModifiedTime
}

func (batch *AccountActivityBatch) LoadAACFile(filename string) (count int) {
	l := list.New()
	file, e := os.Open(filename)
	if e != nil {
		log.Fatal("File error:" + e.Error())
		os.Exit(3)
	}
	defer file.Close()
	scanner := bufio.NewScanner(file)
	count = 0
	for scanner.Scan() {
		buffer := scanner.Bytes()
		var aac AAC
		json.Unmarshal(buffer, aac)
		var activity AccountActivity
		activity.LoadData(aac)
		l.PushBack(activity)
		count++
	}

	batch.Batch = make([]AccountActivity, count)
	i := 0
	for e := l.Front(); e != nil; e = e.Next() {
		if act, ok := e.Value.(AccountActivity); ok {
			batch.Batch[i] = act
		}
		i++
	}

	batch.Count = count

	return
}
