package transactions_hashes

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
	"strings"
	"time"
)

func getTopicBoundariesForLogs(collection collections.Collection, dbInstance *mongo.Database) (map[string]*collections.CollectionInfoLogTopic, error) {
	if collection == collections.CollectionDcl {
		return getTopicBoundariesForLogsFromDatabase(collection, dbInstance)
	}
	return make(map[string]*collections.CollectionInfoLogTopic), nil
}

func getLogsBuildParams(addresses []string, topic string, bnInterval []uint64) ([]byte, error) {
	reqParams := map[string]any{}
	reqParams["address"] = addresses
	reqParams["topics"] = []string{topic}
	reqParams["fromBlock"] = hexutil.EncodeUint64(bnInterval[0])
	if bnInterval[1] > 0 {
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

func getLogsBuildBaseUrl(blockchain string) string {
	if blockchain == collections.PolygonBlockchain {
		return "https://polygon-mainnet.infura.io/v3"
	}
	return "https://mainnet.infura.io/v3"
}

func getEthEventsLogsReq(logTopicInfo *collections.CollectionInfoLogTopic, bnInterval []uint64) (*helpers.EthResponse, error) {
	if logTopicInfo == nil || len(logTopicInfo.Contracts) == 0 {
		return nil, errors.New("no token contracts found")
	}

	baseUrl := getLogsBuildBaseUrl(logTopicInfo.Blockchain)
	url := fmt.Sprintf("%s/%s", baseUrl, os.Getenv("INFURA_API_KEY"))

	payload, err := getLogsBuildParams(logTopicInfo.Contracts, logTopicInfo.Hash, bnInterval)
	if err != nil {
		return nil, err
	}
	response := &helpers.EthResponse{}
	err = helpers.PostData(url, "", payload, response)

	return response, err
}

func handleEthEventsResponse(logTopicInfo *collections.CollectionInfoLogTopic, response *helpers.EthResponse) ([]*helpers.EthEventLog, []uint64, error) {
	if response.Error != nil {
		message := "an error occurred on fetching data from Infura API !"
		if reflect.TypeOf(response.Error).Kind() == reflect.Map {
			code := response.Error.(map[string]interface{})["code"].(float64)
			message = response.Error.(map[string]interface{})["message"].(string)
			if code == -32005 && strings.Contains(message, "query returned more than 10000 results") {
				errorStr, err := json.Marshal(response.Error)
				if err != nil {
					return nil, nil, err
				}
				errorPayload := &helpers.EthBlockRangeError{}
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
			}
		}
		return nil, nil, errors.New(message)
	} else {
		var events []*helpers.EthEventLog
		resJson, err := json.Marshal(response.Result)
		if err != nil {
			return nil, nil, err
		}
		err = json.Unmarshal(resJson, &events)
		if err != nil {
			return nil, nil, err
		}
		for _, event := range events {
			event.Blockchain = &logTopicInfo.Blockchain
		}
		return events, nil, nil
	}
}

func getEthEventsLogsOfTopic(logTopicInfo *collections.CollectionInfoLogTopic, latestFetchedBlockNumber uint64) ([]*helpers.EthEventLog, uint64, error) {

	response, err := getEthEventsLogsReq(logTopicInfo, []uint64{latestFetchedBlockNumber + 1, 0})
	if err != nil {
		return nil, 0, err
	}
	events, bnInterval, err := handleEthEventsResponse(logTopicInfo, response)
	if err != nil {
		return nil, 0, err
	} else if bnInterval != nil {
		response, err = getEthEventsLogsReq(logTopicInfo, bnInterval)
		if err != nil {
			return nil, 0, err
		}
		events, _, err = handleEthEventsResponse(logTopicInfo, response)
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
