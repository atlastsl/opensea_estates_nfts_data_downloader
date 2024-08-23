package transactions_logs

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"strings"
	"sync"
	"time"
)

func parseEthEventLog(eventLog *helpers.EthEventLog) *transactions_infos.TransactionLog {
	if eventLog.Address != nil && eventLog.Data != nil {
		blockNumber, _ := helpers.HexConvertToInt(*eventLog.BlockNumber)
		logIndex, _ := helpers.HexConvertToInt(*eventLog.LogIndex)
		transactionIndex, _ := helpers.HexConvertToInt(*eventLog.TransactionIndex)
		cleanTopics := helpers.ArrayMap(eventLog.Topics, func(t string) (bool, string) {
			return true, helpers.HexRemoveLeadingZeros(t)
		}, true, "")
		txLog := &transactions_infos.TransactionLog{}
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

func saveTransactionInfo(txLogs []*transactions_infos.TransactionLog) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveTransactionsLogsInDatabase(txLogs, dbInstance)
	return err
}

func parseTransactionLogs(logs []*helpers.EthEventLog, wg *sync.WaitGroup) error {
	txLogs := make([]*transactions_infos.TransactionLog, 0)
	for _, log := range logs {
		txLog := parseEthEventLog(log)
		if txLog != nil {
			txLogs = append(txLogs, txLog)
		}
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = saveTransactionInfo(txLogs)
	}()

	return nil
}
