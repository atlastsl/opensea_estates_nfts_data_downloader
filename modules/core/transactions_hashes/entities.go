package transactions_hashes

import (
	"github.com/kamva/mgm/v3"
	"time"
)

type TransactionHash struct {
	mgm.DefaultModel
	Collection      string    `bson:"collection,omitempty"`
	TransactionHash string    `bson:"transaction_hash,omitempty"`
	BlockNumber     int       `bson:"block_number,omitempty"`
	BlockHash       string    `bson:"block_hash,omitempty"`
	BlockTimestamp  time.Time `bson:"block_timestamp,omitempty"`
}
