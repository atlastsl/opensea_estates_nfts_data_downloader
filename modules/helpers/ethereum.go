package helpers

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"reflect"
	"time"
)

type EthEventLog struct {
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

type EthBlockInfo struct {
	BlockNumber    int64     `bigquery:"block_number"`
	BlockTimestamp time.Time `bigquery:"block_timestamp"`
}

type EthTransaction struct {
	BlockHash        *string `json:"blockHash"`
	BlockNumber      *string `json:"blockNumber"`
	ChainID          *string `json:"chainId"`
	From             *string `json:"from"`
	Gas              *string `json:"gas"`
	GasPrice         *string `json:"gasPrice"`
	Hash             *string `json:"hash"`
	Input            *string `json:"input"`
	Nonce            *string `json:"nonce"`
	R                *string `json:"r"`
	S                *string `json:"s"`
	To               *string `json:"to"`
	TransactionIndex *string `json:"transactionIndex"`
	Type             *string `json:"type"`
	V                *string `json:"v"`
	Value            *string `json:"value"`
}

type EthTransactionReceipt struct {
	BlockHash         *string       `json:"blockHash"`
	BlockNumber       *string       `json:"blockNumber"`
	ContractAddress   *string       `json:"contractAddress"`
	CumulativeGasUsed *string       `json:"cumulativeGasUsed"`
	EffectiveGasPrice *string       `json:"effectiveGasPrice"`
	From              *string       `json:"from"`
	GasUsed           *string       `json:"gasUsed"`
	Logs              []EthEventLog `json:"logs"`
	LogsBloom         *string       `json:"logsBloom"`
	Status            *string       `json:"status"`
	To                *string       `json:"to"`
	TransactionHash   *string       `json:"transactionHash"`
	TransactionIndex  *string       `json:"transactionIndex"`
	Type              *string       `json:"type"`
}

func InfuraRequest(payload map[string]any, result interface{}) error {
	rv := reflect.ValueOf(result)
	if rv.Kind() != reflect.Pointer || rv.IsNil() {
		return errors.New("invalid result handler. Must be a pointer to a non nil value")
	}

	payloadStr, err := json.MarshalIndent(payload, "", "  ")
	if err != nil {
		return err
	}
	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", os.Getenv("INFURA_API_KEY"))

	response := &EthResponse{}
	err = PostData(url, "", payloadStr, response)
	if err != nil {
		return err
	}

	if response.Error != nil {
		message := "an error occurred on fetching data from Infura API !"
		if reflect.TypeOf(response.Error).Kind() == reflect.Map {
			code := response.Error.(map[string]interface{})["code"].(float64)
			message = response.Error.(map[string]interface{})["message"].(string)
			message = fmt.Sprintf("<%f> %s", code, message)
		}
		return errors.New(message)
	}

	resJson, err := json.Marshal(response.Result)
	if err != nil {
		return err
	}
	err = json.Unmarshal(resJson, result)
	if err != nil {
		return err
	}

	return nil
}
