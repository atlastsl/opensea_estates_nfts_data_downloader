package eth_events

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"math/big"
	"os"
	"strings"
	"time"
)

func buildNextBlockNumber(latestBlock string) string {
	bigLastBlock, _ := hexutil.DecodeBig(latestBlock)
	nextBlock := big.NewInt(0)
	nextBlock.Add(bigLastBlock, big.NewInt(1))
	return hexutil.EncodeBig(nextBlock)
}

func getLogsBuildParams(addresses []string, topic, latestBlock, fromBlock, toBlock string) ([]byte, error) {
	reqParams := map[string]any{}
	reqParams["address"] = addresses
	reqParams["topics"] = []string{topic}
	if toBlock != "" {
		reqParams["toBlock"] = toBlock
	} else {
		reqParams["toBlock"] = "latest"
	}
	if fromBlock != "" {
		reqParams["fromBlock"] = fromBlock
	} else if latestBlock != "" {
		reqParams["fromBlock"] = buildNextBlockNumber(latestBlock)
	} else {
		reqParams["fromBlock"] = "earliest"
	}
	payload := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getLogs",
		"id":      time.Now().UnixMilli(),
		"params":  reqParams,
	}
	return json.Marshal(payload)
}

func getEthEventsLogs(addresses []string, topic, latestBlock string) (map[string][]EthEventRes, string, error) {

	url := fmt.Sprintf("https://mainnet.infura.io/v3/%s", os.Getenv("INFURA_API_KEY"))

	resp1 := &EthResponse{}
	payload, err := getLogsBuildParams(addresses, topic, latestBlock, "", "")
	if err != nil {
		return nil, "", err
	}
	err = helpers.PostData(url, "", payload, resp1)
	if err != nil {
		return nil, "", err
	}

	var result any
	if resp1.Error != nil {
		errorMap := resp1.Error.(map[string]interface{})
		errorData, _ := errorMap["data"]
		errorCode := errorMap["code"].(int)
		errorMessage := errorMap["message"].(string)
		isError := false
		fromBlock, toBlock := "", ""
		if errorCode == -32005 || errorData != nil {
			errorDataMap := errorData.(map[string]interface{})
			varFromBlock, hasFrom := errorDataMap["fromBlock"]
			varToBlock, hasTo := errorDataMap["fromBlock"]
			if !hasFrom {
				isError = true
			} else {
				fromBlock = varFromBlock.(string)
				if hasTo {
					toBlock = varToBlock.(string)
				}
			}
		} else {
			isError = true
		}
		if isError {
			return nil, "", errors.New(strings.ToLower(errorMessage))
		}

		payload, err = getLogsBuildParams(addresses, topic, latestBlock, fromBlock, toBlock)
		if err != nil {
			return nil, "", err
		}
		resp2 := &EthResponse{}
		err = helpers.PostData(url, "", payload, resp2)
		if err != nil {
			return nil, "", err
		} else if resp2.Error != nil {
			errorMap = resp2.Error.(map[string]interface{})
			errorMessage = errorMap["message"].(string)
			return nil, "", errors.New(strings.ToLower(errorMessage))
		} else {
			result = resp2.Result
		}
	} else {
		result = resp1.Result
	}

	var events []EthEventRes
	resJson, err := json.Marshal(result)
	if err != nil {
		return nil, "", err
	}

	err = json.Unmarshal(resJson, &events)
	if err != nil {
		return nil, "", err
	}

	evMap := map[string][]EthEventRes{}
	for _, event := range events {
		_, ok := evMap[*event.BlockHash]
		if !ok {
			evMap[*event.BlockHash] = make([]EthEventRes, 0)
		}
		evMap[*event.BlockHash] = append(evMap[*event.BlockHash], event)
	}

	newLatestBlock := ""
	if len(events) > 0 {
		newLatestBlock = *events[len(events)-1].BlockNumber
	}

	return evMap, newLatestBlock, nil
}

func getEthEvents(collection collections.Collection, topic, latestBlock string) (map[string][]EthEventRes, string, error) {
	addresses := getAddresses(collection)
	if len(addresses) == 0 {
		return nil, "", errors.New("no addresses found")
	}
	return getEthEventsLogs(addresses, topic, latestBlock)
}
