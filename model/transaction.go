package model

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"
	"path"
	"strconv"

	mgo "gopkg.in/mgo.v2"
	"gopkg.in/mgo.v2/bson"

	"../common"
	"../fs"
)

// Transaction - represents a transaction with properties used in submission DA
type Transaction struct {
	MRN             string
	TransactionType string
	InternalMRN     string
	SOR             string
	Partner         string
	Date            uint32
}

// LoadTxFile - load, process and delete files
func LoadTxFile(txDir string, cTx *mgo.Collection) (e error) {
	// Load transactions
	files := fs.LoadFilesByTime(txDir)
	for _, file := range files {
		txfile := path.Join(txDir, file.Name())
		_, txErr := os.Stat(txfile)
		if txErr == nil {
			println("Loading Tx from", file.Name())
			transactions := loadTx(txfile, cTx)
			println("Loaded", transactions, "transactions.")
			fs.DeleteFilesWithSuffix(txDir, file.Name())
			println("Deleted tx.csv")
		} else {
			e = txErr
		}
	}
	return
}

func loadTx(filepath string, col *mgo.Collection) (total int) {
	f, e := os.Open(filepath)
	if e != nil {
		log.Fatal(e)
	}
	reader := bufio.NewReader(f)
	r := csv.NewReader(reader)
	total = 0
	var i uint32
	transactions := make([]interface{}, util.TxBufferSize)
	for {
		record, err := r.Read()
		if err == io.EOF {
			if i > 0 {
				saveTx(transactions[:i], col)
			}
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		var transaction Transaction
		transaction.LoadData(record)
		transactions[i] = transaction
		i++
		total++
		if i >= util.TxBufferSize {
			saveTx(transactions, col)
			transactions = make([]interface{}, util.TxBufferSize)
			i = 0
		}
	}
	return
}

func saveTx(transactions []interface{}, col *mgo.Collection) {
	println("Saving", len(transactions), "transactions...")
	bulk := col.Bulk()
	bulk.Unordered()
	bulk.Insert(transactions...)
	_, e := bulk.Run()
	if e != nil {
		log.Fatal(e)
	}
	println("Saved", len(transactions), "transactions.")
}

// LoadData - loads data from deserialized csv record
func (t *Transaction) LoadData(record []string) {
	if len(record) >= 4 {
		t.MRN = record[0]
		v, e := strconv.ParseUint(record[1], 10, 16)
		if e != nil {
			log.Fatal(e)
		}
		t.TransactionType = util.TransactionTypeIDToStr(uint16(v))
		internalMRN := record[2]
		if internalMRN == "#" {
			t.InternalMRN = t.MRN
		} else {
			t.InternalMRN = internalMRN
		}
		t.SOR = record[3]
		t.Partner = record[4]
		var d uint64
		d, e = strconv.ParseUint(record[5], 10, 32)
		t.Date = uint32(d)
		if e != nil {
			log.Fatal(e)
		}
	}
}

// GetTxHashCode - get transaction hash
func GetTxHashCode(mrn string, transactiontype string) uint32 {
	s := mrn + "-" + transactiontype
	hash := util.Hash(s)
	return hash
}

// ReadTxFromStore - Load transactions of the same date to a hash map using mrn and tx type as combined key
func ReadTxFromStore(date uint32, col *mgo.Collection) map[uint32]Transaction {
	var transactions []Transaction
	col.Find(bson.M{"date": date}).All(&transactions)
	hashmap := make(map[uint32]Transaction)
	for _, tx := range transactions {
		hash := GetTxHashCode(tx.MRN, tx.TransactionType)
		hashmap[hash] = tx
	}
	return hashmap
}
