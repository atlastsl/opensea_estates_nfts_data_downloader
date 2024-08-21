package transactions_logs

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"strings"
	"sync"
	"time"
)

func dclParseEventTopic(address string, topics []string, cltInfo *collections.CollectionInfo) *transactions_infos.TransactionLogInfo {
	eventHex := topics[0]
	var eventParams *transactions_infos.TransactionLogInfo
	if cltInfo.HasAsset(address) {
		logTopic := cltInfo.GetLogTopic(address, eventHex)
		if logTopic != nil {
			eventParams = &transactions_infos.TransactionLogInfo{}
			eventParams.EventName = logTopic.Name
			if logTopic.Name == "TransferAsset" {
				eventParams.From = helpers.HexRemoveLeadingZeros(topics[1])
				eventParams.To = helpers.HexRemoveLeadingZeros(topics[2])
				eventParams.Asset, _ = helpers.HexConvertToString(topics[3])
			} else if logTopic.Name == "AddLandInEstate" {
				eventParams.Estate, _ = helpers.HexConvertToString(topics[1])
				eventParams.Land, _ = helpers.HexConvertToString(topics[2])
			} else if logTopic.Name == "RemoveLandFromEstate" {
				eventParams.Estate, _ = helpers.HexConvertToString(topics[1])
				eventParams.Land, _ = helpers.HexConvertToString(topics[2])
				eventParams.To = helpers.HexRemoveLeadingZeros(topics[3])
			}
		}
	}
	return eventParams
}

func parseEthEventLog(eventLog *helpers.EthEventLog, cltInfo *collections.CollectionInfo) *transactions_infos.TransactionLog {
	if eventLog.Address != nil && eventLog.Data != nil {
		var txLogInfo *transactions_infos.TransactionLogInfo
		if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
			txLogInfo = dclParseEventTopic(*eventLog.Address, eventLog.Topics, cltInfo)
		}
		if txLogInfo != nil {
			blockNumber, _ := helpers.HexConvertToInt(*eventLog.BlockNumber)
			logIndex, _ := helpers.HexConvertToInt(*eventLog.LogIndex)
			transactionIndex, _ := helpers.HexConvertToInt(*eventLog.TransactionIndex)
			cleanTopics := helpers.ArrayMap(eventLog.Topics, func(t string) (bool, string) {
				return true, helpers.HexRemoveLeadingZeros(t)
			}, true, "")
			txLog := &transactions_infos.TransactionLog{}
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

func saveTransactionInfo(txLogs []*transactions_infos.TransactionLog) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveTransactionsLogsInDatabase(txLogs, dbInstance)
	return err
}

func parseTransactionLogs(logs []*helpers.EthEventLog, cltInfo *collections.CollectionInfo, wg *sync.WaitGroup) error {
	txLogs := make([]*transactions_infos.TransactionLog, 0)
	for _, log := range logs {
		txLog := parseEthEventLog(log, cltInfo)
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
