package model

// Transaction - represents a transaction with properties used in submission DA
type Transaction struct {
	MRN         string
	InternalMRN string
	SOR         string
	Partner     string
}

// LoadData - loads data from deserialized csv record
func (t *Transaction) LoadData(record []string) {
	if len(record) >= 4 {
		t.MRN = record[0]
		t.InternalMRN = record[1]
		t.SOR = record[2]
		t.Partner = record[3]
	}
}
