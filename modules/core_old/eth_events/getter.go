package eth_events

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"reflect"
	"time"
)

const (
	EthereumChain = "ethereum"
)

func getLatestFetchedBlockNumbers(collection collections.Collection, dbInstance *mongo.Database) (map[string]uint64, error) {
	if collection == collections.CollectionDcl {
		return getLatestFetchedBlockNumber(collection, EthereumChain, dbInstance)
	}
	return map[string]uint64{}, nil
}

func getLogsBuildParams(addresses []string, topic string, bnInterval []uint64) ([]byte, error) {
	reqParams := map[string]any{}
	reqParams["address"] = addresses
	reqParams["topics"] = []string{topic}
	reqParams["fromBlock"] = hexutil.EncodeUint64(bnInterval[0])
	if len(bnInterval) > 1 {
		reqParams["toBlock"] = hexutil.EncodeUint64(bnInterval[1])
	} else {
		reqParams["toBlock"] = "latest"
	}
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"id":      time.Now().UnixMilli(),
		"params":  []any{reqParams},
	}
	return json.MarshalIndent(payload, "", "  ")
}

func getEthEventsLogsReq(collection collections.Collection, topic string, bnInterval []uint64) (*EthResponse, error) {
	addresses := getAddresses(collection)
	if len(addresses) == 0 {
		return nil, errors.New("no addresses found")
	}

	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", os.Getenv("INFURA_API_KEY"))

	payload, err := getLogsBuildParams(addresses, topic, bnInterval)
	if err != nil {
		return nil, err
	}
	response := &EthResponse{}
	err = helpers.PostData(url, "", payload, response)

	return response, err
}

func handleEthEventsResponse(response *EthResponse) ([]*EthEventRes, []uint64, error) {
	if response.Error != nil {
		message := "an error occurred on fetching data from Infura API !"
		if reflect.TypeOf(response.Error).Kind() == reflect.Map {
			code := response.Error.(map[string]interface{})["code"].(float64)
			if code == -32005 {
				errorStr, err := json.Marshal(response.Error)
				if err != nil {
					return nil, nil, err
				}
				errorPayload := &EthBlockRangeError{}
				err = json.Unmarshal(errorStr, errorPayload)
				if err != nil {
					return nil, nil, err
				}
				//v, _ := json.MarshalIndent(errorPayload, "", "  ")
				//println(string(errorStr))
				bnFrom, err := hexutil.DecodeUint64(errorPayload.Data.From)
				if err != nil {
					return nil, nil, err
				}
				bnTo, err := hexutil.DecodeUint64(errorPayload.Data.To)
				if err != nil {
					return nil, nil, err
				}
				return nil, []uint64{bnFrom, bnTo}, nil
			} else {
				message = response.Error.(map[string]interface{})["message"].(string)
			}
		}
		return nil, nil, errors.New(message)
	} else {
		var events []*EthEventRes
		resJson, err := json.Marshal(response.Result)
		if err != nil {
			return nil, nil, err
		}
		err = json.Unmarshal(resJson, events)
		if err != nil {
			return nil, nil, err
		}
		return events, nil, nil
	}
}

func getEthEventsLogsOfTopic(collection collections.Collection, topic string, latestFetchedBlockNumber uint64) ([]*EthEventRes, uint64, error) {

	response, err := getEthEventsLogsReq(collection, topic, []uint64{latestFetchedBlockNumber + 1})
	if err != nil {
		return nil, 0, err
	}
	events, bnInterval, err := handleEthEventsResponse(response)
	if err != nil {
		return nil, 0, err
	} else if bnInterval != nil {
		response, err = getEthEventsLogsReq(collection, topic, bnInterval)
		if err != nil {
			return nil, 0, err
		}
		events, _, err = handleEthEventsResponse(response)
		if err != nil {
			return nil, 0, err
		}
	}

	nextLFBlockNumber := latestFetchedBlockNumber
	if len(events) > 0 {
		tmp := *events[len(events)-1].BlockNumber
		nextLFBlockNumber, err = hexutil.DecodeUint64(tmp)
	}

	return events, nextLFBlockNumber, err
}
