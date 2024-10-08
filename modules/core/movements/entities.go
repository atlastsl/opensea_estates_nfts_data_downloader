package movements

import (
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
)

type AssetMovement struct {
	mgm.DefaultModel `bson:",inline"`
	AssetRef         primitive.ObjectID `bson:"asset_ref,omitempty"`
	AssetCollection  string             `bson:"asset_collection,omitempty"`
	AssetContract    string             `bson:"asset_contract,omitempty"`
	AssetIdentifier  string             `bson:"asset_identifier,omitempty"`
	Movement         string             `bson:"movement,omitempty"`
	TxHash           string             `bson:"tx_hash,omitempty"`
	Exchange         string             `bson:"exchange,omitempty"`
	Chain            string             `bson:"chain,omitempty"`
	MvtDate          time.Time          `bson:"mvt_date,omitempty"`
	Sender           string             `bson:"sender,omitempty"`
	Recipient        string             `bson:"recipient,omitempty"`
	Quantity         int64              `bson:"quantity,omitempty"`
	Value            float64            `bson:"value,omitempty"`
	Currency         string             `bson:"currency,omitempty"`
	CcyPrice         float64            `bson:"ccy_price,omitempty"`
	ValueUsd         float64            `bson:"value_usd,omitempty"`
}

type CurrencyPrice struct {
	mgm.DefaultModel `bson:",inline"`
	Start            time.Time `bson:"start,omitempty"`
	End              time.Time `bson:"end,omitempty"`
	Currency         string    `bson:"currency,omitempty"`
	Open             float64   `bson:"open,omitempty"`
	High             float64   `bson:"high,omitempty"`
	Low              float64   `bson:"low,omitempty"`
	Close            float64   `bson:"close,omitempty"`
	Volume           float64   `bson:"volume,omitempty"`
	MarketCap        float64   `bson:"market_cap,omitempty"`
}
