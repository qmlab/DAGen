package main

import (
	"bufio"
	"encoding/csv"
	"io"
	"log"
	"os"

	"../model"
)

func main() {
	file := "e:\\tests\\Tx\\tx.csv"
	f, err := os.Open(file)
	check(err)
	reader := bufio.NewReader(f)
	r := csv.NewReader(reader)
	var transactions []model.Transaction
	for {
		record, err := r.Read()
		if err == io.EOF {
			break
		}
		if err != nil {
			log.Fatal(err)
		}

		var transaction model.Transaction
		transaction.LoadData(record)
		transactions = append(transactions, transaction)
	}
}

func check(e error) {
	if e != nil {
		panic(e)
	}
}
