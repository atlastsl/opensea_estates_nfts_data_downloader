package ops_events

import "github.com/kamva/mgm/v3"

type EstateEvent struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string  `bson:"collection,omitempty"`
	Contract         string  `bson:"contract,omitempty"`
	AssetId          string  `bson:"asset_id,omitempty"`
	Transaction      string  `bson:"transaction,omitempty"`
	FixedTransaction string  `bson:"fixed_transaction,omitempty"`
	EventType        string  `bson:"event_type,omitempty"`
	Exchange         string  `bson:"exchange,omitempty"`
	Chain            string  `bson:"chain,omitempty"`
	TxTimestamp      int64   `bson:"tx_timestamp,omitempty"`
	EvtTimestamp     int64   `bson:"evt_timestamp,omitempty"`
	Sender           string  `bson:"from,omitempty"`
	Recipient        string  `bson:"to,omitempty"`
	Seller           string  `bson:"seller,omitempty"`
	Buyer            string  `bson:"buyer,omitempty"`
	Quantity         int64   `bson:"quantity,omitempty"`
	Amount           float64 `bson:"amount,omitempty"`
	Currency         string  `bson:"currency,omitempty"`
	CcyAddress       string  `bson:"ccy_address,omitempty"`
	CCyDecimals      int64   `bson:"ccy_decimals,omitempty"`
}
