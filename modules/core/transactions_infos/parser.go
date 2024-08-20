package transactions_infos

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/helpers"
	"os"
	"strings"
	"sync"
	"time"
)

func getTransactionByHash(transactionHash string) (*helpers.EthTransaction, error) {
	payloadMap := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionByHash",
		"id":      time.Now().UnixMilli(),
		"params":  []string{transactionHash},
	}
	txInfo := &helpers.EthTransaction{}
	err := helpers.InfuraRequest(payloadMap, txInfo)
	if err != nil {
		return nil, err
	}
	return txInfo, nil
}

func getTransactionReceipt(transactionHash string) (*helpers.EthTransactionReceipt, error) {
	payloadMap := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getTransactionReceipt",
		"id":      time.Now().UnixMilli(),
		"params":  []string{transactionHash},
	}
	txReceipt := &helpers.EthTransactionReceipt{}
	err := helpers.InfuraRequest(payloadMap, txReceipt)
	if err != nil {
		return nil, err
	}
	return txReceipt, nil
}

func dclParseEventTopic(address string, topics []string, data string, cltInfo *collections.CollectionInfo) *TransactionLogInfo {
	eventHex := topics[0]
	var eventParams *TransactionLogInfo
	if cltInfo.HasAsset(address) {
		logTopic := cltInfo.GetLogTopic(address, eventHex)
		if logTopic != nil {
			eventParams = &TransactionLogInfo{}
			eventParams.EventName = logTopic.Name
			if logTopic.Name == "TransferAsset" {
				eventParams.From = helpers.HexRemoveLeadingZeros(topics[1])
				eventParams.To = helpers.HexRemoveLeadingZeros(topics[2])
				eventParams.Asset, _ = helpers.HexConvertToString(topics[3])
			} else if logTopic.Name == "AddLandInEstate" {
				eventParams.Estate, _ = helpers.HexConvertToString(topics[1])
				eventParams.Land, _ = helpers.HexConvertToString(topics[2])
			} else if logTopic.Name == "AddLandOutEstate" {
				eventParams.Estate, _ = helpers.HexConvertToString(topics[1])
				eventParams.Land, _ = helpers.HexConvertToString(topics[2])
				eventParams.To = helpers.HexRemoveLeadingZeros(topics[3])
			}
		}
	} else {
		if eventHex == os.Getenv("ETH_TRANSFER_LOG_HEX") && len(topics) == 3 && data != "" {
			eventParams = &TransactionLogInfo{}
			eventParams.EventName = os.Getenv("ETH_TRANSFER_LOG_MONEY")
			eventParams.From = helpers.HexRemoveLeadingZeros(topics[1])
			eventParams.To = helpers.HexRemoveLeadingZeros(topics[2])
			eventParams.Amount, _ = helpers.HexConvertToString(data)
		}
	}
	return eventParams
}

func parseEthEventLog(eventLog *helpers.EthEventLog, cltInfo *collections.CollectionInfo) *TransactionLog {
	if eventLog.Address != nil && eventLog.Data != nil {
		var txLogInfo *TransactionLogInfo
		if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
			txLogInfo = dclParseEventTopic(*eventLog.Address, eventLog.Topics, *eventLog.Data, cltInfo)
		}
		if txLogInfo != nil {
			blockNumber, _ := helpers.HexConvertToInt(*eventLog.BlockNumber)
			logIndex, _ := helpers.HexConvertToInt(*eventLog.LogIndex)
			transactionIndex, _ := helpers.HexConvertToInt(*eventLog.TransactionIndex)
			cleanTopics := helpers.ArrayMap(eventLog.Topics, func(t string) (bool, string) {
				return true, helpers.HexRemoveLeadingZeros(t)
			}, true, "")
			txLog := &TransactionLog{}
			txLog.CreatedAt = time.Now()
			txLog.UpdatedAt = time.Now()
			txLog.Collection = cltInfo.Name
			txLog.TransactionHash = *eventLog.TransactionHash
			txLog.Address = *eventLog.Address
			txLog.TransactionIndex = transactionIndex
			txLog.Topics = cleanTopics
			txLog.EventId = strings.Join(cleanTopics, "-")
			txLog.BlockHash = *eventLog.BlockHash
			txLog.BlockNumber = blockNumber
			txLog.Data = *eventLog.Data
			txLog.LogIndex = logIndex
			txLog.Removed = *eventLog.Removed
			txLog.EventName = txLogInfo.EventName
			txLog.EventParams = txLogInfo
			return txLog
		}
	}
	return nil
}

func parseTransactionLogs(logs []helpers.EthEventLog, cltInfo *collections.CollectionInfo) []*TransactionLog {
	txLogs := make([]*TransactionLog, 0)
	for _, log := range logs {
		txLog := parseEthEventLog(&log, cltInfo)
		if txLog != nil {
			txLogs = append(txLogs, txLog)
		}
	}
	return txLogs
}

func parseTransactionInfo(transactionHash *transactions_hashes.TransactionHash, txDetails *helpers.EthTransaction, txReceipt *helpers.EthTransactionReceipt, cltInfo *collections.CollectionInfo) (*TransactionInfo, []*TransactionLog) {
	txInfo := &TransactionInfo{}
	txInfo.Collection = cltInfo.Name
	txInfo.TransactionHash = transactionHash.TransactionHash
	txInfo.BlockNumber, _ = helpers.HexConvertToInt(*txDetails.BlockNumber)
	txInfo.BlockHash = *txDetails.BlockHash
	txInfo.BlockTimestamp = transactionHash.BlockTimestamp
	if txDetails.ChainID != nil {
		txInfo.ChainID, _ = helpers.HexConvertToString(*txDetails.ChainID)
	}
	txInfo.Gas, _ = helpers.HexConvertToString(*txDetails.Gas)
	txInfo.GasUsed, _ = helpers.HexConvertToString(*txReceipt.GasUsed)
	txInfo.CumulativeGasUsed, _ = helpers.HexConvertToString(*txReceipt.CumulativeGasUsed)
	txInfo.GasPrice, _ = helpers.HexConvertToString(*txReceipt.EffectiveGasPrice)
	txInfo.From = *txDetails.From
	txInfo.To = *txDetails.To
	txInfo.Value, _ = helpers.HexConvertToString(*txDetails.Value)
	txInfo.TransactionIndex, _ = helpers.HexConvertToInt(*txDetails.TransactionIndex)
	txInfo.Input = *txDetails.Input
	txInfo.Nonce, _ = helpers.HexConvertToInt(*txDetails.Nonce)
	txInfo.R = *txDetails.R
	txInfo.S = *txDetails.S
	txInfo.V, _ = helpers.HexConvertToString(*txDetails.V)
	txInfo.Type, _ = helpers.HexConvertToString(*txDetails.Type)
	txInfo.Status, _ = helpers.HexConvertToString(*txReceipt.Status)
	txLogs := parseTransactionLogs(txReceipt.Logs, cltInfo)
	return txInfo, txLogs
}

func convertTxHashToTxInfo(txHash *transactions_hashes.TransactionHash, cltInfo *collections.CollectionInfo) (*TransactionInfo, []*TransactionLog, error) {
	txDetails, err := getTransactionByHash(txHash.TransactionHash)
	if err != nil {
		return nil, nil, err
	}
	txReceipt, err := getTransactionReceipt(txHash.TransactionHash)
	if err != nil {
		return nil, nil, err
	}
	txInfo, tTxLogs := parseTransactionInfo(txHash, txDetails, txReceipt, cltInfo)
	return txInfo, tTxLogs, err
}

func saveTransactionInfo(txInfos []*TransactionInfo, txLogs []*TransactionLog) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveTransactionsInfosDatabase(txInfos, dbInstance)
	if err != nil {
		return err
	}
	err = saveTransactionsLogsInDatabase(txLogs, dbInstance)
	return err
}

func parseTransactionsInfo(transactionsHashes []*transactions_hashes.TransactionHash, cltInfo *collections.CollectionInfo, wg *sync.WaitGroup) error {
	txInfos := make([]*TransactionInfo, 0)
	txLogs := make([]*TransactionLog, 0)
	allErrors := make([]error, 0)

	parserWg := &sync.WaitGroup{}
	dataLocker := &sync.RWMutex{}
	for _, txHash := range transactionsHashes {
		parserWg.Add(1)
		go func() {
			defer parserWg.Done()
			dataLocker.Lock()
			txInfo, tTxLogs, err := convertTxHashToTxInfo(txHash, cltInfo)
			if err != nil {
				allErrors = append(allErrors, err)
			} else {
				txInfos = append(txInfos, txInfo)
				txLogs = append(txLogs, tTxLogs...)
			}
			dataLocker.Unlock()
		}()
	}

	if len(allErrors) > 0 {
		return allErrors[0]
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = saveTransactionInfo(txInfos, txLogs)
	}()

	return nil
}
