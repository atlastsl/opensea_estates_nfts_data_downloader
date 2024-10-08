package eth_events

import "github.com/kamva/mgm/v3"

type EthEventRes struct {
	Address          *string  `json:"address"`
	BlockHash        *string  `json:"blockHash"`
	BlockNumber      *string  `json:"blockNumber"`
	Data             *string  `json:"data"`
	LogIndex         *string  `json:"logIndex"`
	Removed          *bool    `json:"removed"`
	Topics           []string `json:"topics"`
	TransactionHash  *string  `json:"transactionHash"`
	TransactionIndex *string  `json:"transactionIndex"`
}

type EthEvent struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string         `bson:"collection,omitempty"`
	Address          string         `bson:"address,omitempty"`
	EventId          string         `bson:"event_id,omitempty"`
	BlockHash        string         `bson:"block_hash,omitempty"`
	BlockNumber      int            `bson:"block_number,omitempty"`
	Data             string         `bson:"data,omitempty"`
	LogIndex         int            `bson:"log_index,omitempty"`
	Removed          bool           `bson:"removed"`
	EventName        string         `bson:"event_name,omitempty"`
	EventParams      map[string]any `bson:"event_params,omitempty"`
	TransactionHash  string         `bson:"transaction_hash,omitempty"`
	TransactionIndex int            `bson:"transaction_index,omitempty"`
}

type EthResponse struct {
	JsonRpc string `json:"jsonrpc"`
	Id      int    `json:"id"`
	Error   any    `json:"error"`
	Result  any    `json:"result"`
}

type EthBlockRangeError struct {
	Code    float64                `json:"code"`
	Message string                 `json:"message"`
	Data    EthBlockRangeErrorData `json:"data"`
}

type EthBlockRangeErrorData struct {
	From  string  `json:"from"`
	Limit float64 `json:"limit"`
	To    string  `json:"to"`
}

type BlockNumber struct {
	mgm.DefaultModel
	Collection    string `json:"collection" bson:"collection,omitempty"`
	Topic         string `json:"topic" bson:"topic,omitempty"`
	Chain         string `json:"chain" bson:"chain,omitempty"`
	LatestFetched uint64 `json:"latest_fetched" bson:"latest_fetched,omitempty"`
}
