package transactions_infos

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/helpers"
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

func parseEthEventLog(eventLog *helpers.EthEventLog) *TransactionLog {
	if eventLog.Address != nil && eventLog.Data != nil {
		blockNumber, _ := helpers.HexConvertToInt(*eventLog.BlockNumber)
		logIndex, _ := helpers.HexConvertToInt(*eventLog.LogIndex)
		transactionIndex, _ := helpers.HexConvertToInt(*eventLog.TransactionIndex)
		cleanTopics := helpers.ArrayMap(eventLog.Topics, func(t string) (bool, string) {
			return true, helpers.HexRemoveLeadingZeros(t)
		}, true, "")
		txLog := &TransactionLog{}
		txLog.CreatedAt = time.Now()
		txLog.UpdatedAt = time.Now()
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
		return txLog
	}
	return nil
}

func parseTransactionLogs(logs []helpers.EthEventLog) []*TransactionLog {
	txLogs := make([]*TransactionLog, 0)
	for _, log := range logs {
		txLog := parseEthEventLog(&log)
		if txLog != nil {
			txLogs = append(txLogs, txLog)
		}
	}
	return txLogs
}

func parseTransactionInfo(transactionHash *transactions_hashes.TransactionHash, txDetails *helpers.EthTransaction, txReceipt *helpers.EthTransactionReceipt, cltInfo *collections.CollectionInfo) (*TransactionInfo, []*TransactionLog) {
	var txInfo *TransactionInfo
	txLogs := make([]*TransactionLog, 0)
	if txDetails != nil && txReceipt != nil {
		txInfo = &TransactionInfo{}
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
	}
	if txReceipt != nil {
		txLogs = parseTransactionLogs(txReceipt.Logs)
	}
	return txInfo, txLogs
}

func convertTxHashToTxInfo(txInput *transactionInput, cltInfo *collections.CollectionInfo) (*TransactionInfo, []*TransactionLog, error) {
	var txDetails *helpers.EthTransaction
	var txReceipt *helpers.EthTransactionReceipt
	var err error
	if txInput.fetchInfo {
		txDetails, err = getTransactionByHash(txInput.txHash.TransactionHash)
	}
	if err != nil {
		return nil, nil, err
	}
	if txInput.fetchLogs {
		txReceipt, err = getTransactionReceipt(txInput.txHash.TransactionHash)
	}
	if err != nil {
		return nil, nil, err
	}
	txInfo, tTxLogs := parseTransactionInfo(txInput.txHash, txDetails, txReceipt, cltInfo)
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

func parseTransactionsInfo(inputs []*transactionInput, cltInfo *collections.CollectionInfo, wg *sync.WaitGroup) error {
	txInfos := make([]*TransactionInfo, 0)
	txLogs := make([]*TransactionLog, 0)
	allErrors := make([]error, 0)

	parserWg := &sync.WaitGroup{}
	dataLocker := &sync.RWMutex{}
	for _, txInput := range inputs {
		parserWg.Add(1)
		go func() {
			defer parserWg.Done()
			dataLocker.Lock()
			txInfo, tTxLogs, err := convertTxHashToTxInfo(txInput, cltInfo)
			if err != nil {
				allErrors = append(allErrors, err)
			} else {
				if txInfo != nil {
					txInfos = append(txInfos, txInfo)
				}
				if tTxLogs != nil && len(tTxLogs) > 0 {
					txLogs = append(txLogs, tTxLogs...)
				}
			}
			dataLocker.Unlock()
		}()
	}
	parserWg.Wait()

	if len(allErrors) > 0 {
		return allErrors[0]
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		err := saveTransactionInfo(txInfos, txLogs)
		if err != nil {
			println(err.Error())
		}
	}()

	/*txInfos := make([]*TransactionInfo, 0)
	txLogs := make([]*TransactionLog, 0)
	for _, txInput := range inputs {
		txInfo, tTxLogs, err := convertTxHashToTxInfo(txInput, cltInfo)
		if err != nil {
			return err
		} else {
			if txInfo != nil {
				txInfos = append(txInfos, txInfo)
			}
			if tTxLogs != nil && len(tTxLogs) > 0 {
				txLogs = append(txLogs, tTxLogs...)
			}
		}
	}*/

	return nil
}
