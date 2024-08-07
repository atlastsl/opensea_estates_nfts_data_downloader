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
	"strings"
	"time"
)

const (
	EthereumChain = "ethereum"
)

const (
	EthereumStep = 25000
)

func getCurrentLastBlockNumber() (uint64, error) {
	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", os.Getenv("INFURA_API_KEY"))
	mPayload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_blockNumber",
		"params":  []string{},
		"id":      time.Now().UnixMilli(),
	}
	payload, err := json.Marshal(mPayload)
	if err != nil {
		return 0, err
	}
	response := &EthResponse{}
	err = helpers.PostData(url, "", payload, response)
	if err != nil {
		return 0, err
	}
	if response.Error != nil {
		sErrorMessage := ""
		errorPayload := response.Error.(map[string]interface{})
		if errorMessage, ok := errorPayload["message"]; ok {
			sErrorMessage = errorMessage.(string)
		} else {
			sErrorMessage = "invalid request"
		}
		return 0, errors.New(sErrorMessage)
	} else if reflect.TypeOf(response.Result).Kind() != reflect.String {
		return 0, errors.New("invalid response")
	} else {
		lBlockNumber, err := helpers.HexConvertToInt(response.Result.(string))
		return uint64(lBlockNumber), err
	}
}

func getSlicesOfBlockNumbers(collection collections.Collection, dbInstance *mongo.Database) ([][]uint64, error) {
	bnSlices := make([][]uint64, 0)
	if collection == collections.CollectionDcl {
		latestFetchedBN, err := getLatestFetchedBlockNumber(collection, EthereumChain, dbInstance)
		if err != nil {
			return nil, err
		}
		latestTrueBN, err := getCurrentLastBlockNumber()
		if err != nil {
			return nil, err
		}
		err = saveLatestTrueBlockNumber(collection, EthereumChain, latestTrueBN, dbInstance)
		if err != nil {
			return nil, err
		}
		if latestFetchedBN < latestTrueBN {
			parcours := latestFetchedBN
			for parcours < latestTrueBN {
				bInf := parcours + 1
				bSup := parcours + EthereumStep
				if bSup > latestTrueBN {
					bSup = latestTrueBN
				}
				bnSlices = append(bnSlices, []uint64{bInf, bSup})
				parcours = bSup
			}
		}
	}
	return bnSlices, nil
}

func getLogsBuildParams(addresses []string, topic string, fromBlock, toBlock uint64) ([]byte, error) {
	reqParams := map[string]any{}
	reqParams["address"] = addresses
	reqParams["topics"] = []string{topic}
	reqParams["fromBlock"] = hexutil.EncodeUint64(fromBlock)
	reqParams["toBlock"] = hexutil.EncodeUint64(toBlock)
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"id":      time.Now().UnixMilli(),
		"params":  []any{reqParams},
	}
	return json.MarshalIndent(payload, "", "  ")
}

func getEthEventsLogsOfTopic(collection collections.Collection, topic string, bnSlice []uint64) ([]*EthEventRes, error) {

	addresses := getAddresses(collection)
	if len(addresses) == 0 {
		return nil, errors.New("no addresses found")
	}

	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", os.Getenv("INFURA_API_KEY"))

	var result any

	payload, err := getLogsBuildParams(addresses, topic, bnSlice[0], bnSlice[1])
	if err != nil {
		return nil, err
	}
	response := &EthResponse{}
	err = helpers.PostData(url, "", payload, response)
	if err != nil {
		return nil, err
	} else if response.Error != nil {
		errorMap := response.Error.(map[string]interface{})
		errorMessage := errorMap["message"].(string)
		return nil, errors.New(strings.ToLower(errorMessage))
	} else {
		result = response.Result
	}

	var events []*EthEventRes
	resJson, err := json.Marshal(result)
	if err != nil {
		return nil, err
	}
	err = json.Unmarshal(resJson, &events)
	if err != nil {
		return nil, err
	}

	return events, nil
}

func getEthEventsLogs(collection collections.Collection, bnSlice []uint64) ([]*EthEventRes, error) {
	events := make([]*EthEventRes, 0)
	topics, _ := getTopicInfo(collection)

	for _, topic := range topics {
		topicEvents, err := getEthEventsLogsOfTopic(collection, topic, bnSlice)
		if err != nil {
			return nil, err
		}
		if len(topicEvents) > 0 {
			events = append(events, topicEvents...)
		}
		//events = slices.Concat(events, topicEvents)
	}

	return events, nil
}
