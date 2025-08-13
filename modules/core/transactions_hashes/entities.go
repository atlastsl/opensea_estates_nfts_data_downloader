package transactions_hashes

import (
	"github.com/kamva/mgm/v3"
	"time"
)

type TransactionHash struct {
	mgm.DefaultModel `bson:",inline"`
	Blockchain       string    `json:"blockchain,omitempty"`
	Metaverse        string    `bson:"metaverse,omitempty"`
	TransactionHash  string    `bson:"transaction_hash,omitempty"`
	BlockNumber      int       `bson:"block_number,omitempty"`
	BlockHash        string    `bson:"block_hash,omitempty"`
	BlockTimestamp   time.Time `bson:"block_timestamp,omitempty"`
}

type SpecificBnItem struct {
	Start uint64 `json:"start"`
	End   uint64 `json:"end"`
	Done  bool   `json:"done"`
}

type SpecificBnDict map[string]map[string][]SpecificBnItem
