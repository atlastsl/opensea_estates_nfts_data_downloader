package operations

import (
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type OperationValue struct {
	Value         float64 `bson:"value,omitempty"`
	Currency      string  `bson:"currency,omitempty"`
	CurrencyPrice float64 `bson:"currency_price,omitempty"`
	ValueUsd      float64 `bson:"value_usd,omitempty"`
}

type MarketDataInfo struct {
	Currency  string  `bson:"currency,omitempty"`
	Price     float64 `bson:"price,omitempty"`
	Change24h float64 `bson:"change_24h,omitempty"`
	Volume24h float64 `bson:"volume_24h,omitempty"`
	MarketCap float64 `bson:"market_cap,omitempty"`
}

type AssetChange struct {
	AttrName       string         `bson:"attr_name,omitempty"`
	AttrDisName    string         `bson:"attr_dis_name,omitempty"`
	DataType       string         `bson:"data_type,omitempty"`
	DataTypeParams map[string]any `bson:"data_type_params,omitempty"`
	Value          string         `bson:"value,omitempty"`
}

type Operation struct {
	mgm.DefaultModel `bson:",inline"`
	Metaverse        string             `bson:"metaverse,omitempty"`
	AssetRef         primitive.ObjectID `bson:"asset,omitempty"`
	AssetContract    string             `bson:"asset_contract,omitempty"`
	AssetId          string             `bson:"asset_id,omitempty"`
	AssetChanges     []AssetChange      `bson:"asset_changes,omitempty"`
	TransactionHash  string             `bson:"transaction_hash,omitempty"`
	OperationType    string             `bson:"operation_type,omitempty"`
	TransactionType  string             `bson:"transaction_type,omitempty"`
	Blockchain       string             `bson:"blockchain,omitempty"`
	BlockNumber      int64              `bson:"block_number,omitempty"`
	BlockHash        string             `bson:"block_hash,omitempty"`
	Date             time.Time          `bson:"date,omitempty"`
	Sender           string             `bson:"sender,omitempty"`
	Recipient        string             `bson:"recipient,omitempty"`
	Amount           []OperationValue   `bson:"amount"`
	Fees             []OperationValue   `bson:"fees"`
	MarketInfo       MarketDataInfo     `bson:"market_info"`
}

const (
	OperationTypeFree       = "free"
	OperationTypeSale       = "sale"
	TransactionTypeMint     = "mint"
	TransactionTypeTransfer = "transfer"
)
