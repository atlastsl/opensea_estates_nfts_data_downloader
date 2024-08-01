package movements

import (
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type AssetMovement struct {
	mgm.DefaultModel
	AssetRef  primitive.ObjectID `bson:"asset_ref,omitempty"`
	Movement  string             `bson:"movement,omitempty"`
	TxHash    string             `bson:"tx_hash,omitempty"`
	Exchange  string             `bson:"exchange,omitempty"`
	Chain     string             `bson:"chain,omitempty"`
	MvtDate   time.Time          `bson:"mvt_date,omitempty"`
	Sender    string             `bson:"sender,omitempty"`
	Recipient string             `bson:"recipient,omitempty"`
	Quantity  int64              `bson:"quantity,omitempty"`
	Value     float64            `bson:"value,omitempty"`
	Currency  string             `bson:"currency,omitempty"`
	ValueUsd  float64            `bson:"value_usd,omitempty"`
}

type CurrencyPrice struct {
	mgm.DefaultModel
	Start     string  `bson:"start,omitempty"`
	End       string  `bson:"end,omitempty"`
	Currency  string  `bson:"currency,omitempty"`
	Open      float64 `bson:"open,omitempty"`
	High      float64 `bson:"high,omitempty"`
	Low       float64 `bson:"low,omitempty"`
	Close     float64 `bson:"close,omitempty"`
	Volume    float64 `bson:"volume,omitempty"`
	MarketCap float64 `bson:"market_cap,omitempty"`
}
