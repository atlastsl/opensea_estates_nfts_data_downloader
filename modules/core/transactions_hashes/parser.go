package transactions_hashes

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"slices"
	"sync"
)

func parseEthEventLog(eventLog *helpers.EthEventLog, collection collections.Collection) *TransactionHash {
	blockNumber, _ := helpers.HexConvertToInt(*eventLog.BlockNumber)
	txHash := &TransactionHash{}
	txHash.Blockchain = *eventLog.Blockchain
	txHash.Collection = string(collection)
	txHash.TransactionHash = *eventLog.TransactionHash
	txHash.BlockHash = *eventLog.BlockHash
	txHash.BlockNumber = blockNumber
	return txHash
}

func filterEthEventLogs(eventLogs []*helpers.EthEventLog) []*helpers.EthEventLog {
	filtered := make([]*helpers.EthEventLog, 0)
	hashes := make([]string, 0)
	for _, log := range eventLogs {
		if !slices.Contains(hashes, *log.TransactionHash) {
			filtered = append(filtered, log)
		}
	}
	return filtered
}

func saveParsedEvents(transactionHashes []*TransactionHash) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveTransactionHashesInDatabase(transactionHashes, dbInstance)
	return err
}

func parseEthEventsRes(eventsLogs []*helpers.EthEventLog, collection collections.Collection, wg *sync.WaitGroup) error {
	filtered := filterEthEventLogs(eventsLogs)

	transactionHashes := helpers.ArrayMap(filtered, func(t *helpers.EthEventLog) (bool, *TransactionHash) {
		return true, parseEthEventLog(t, collection)
	}, true, nil)

	wg.Add(1)
	go func() {
		_ = saveParsedEvents(transactionHashes)
		wg.Done()
	}()

	return nil
}
