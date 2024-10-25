package operations

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"slices"
	"strings"
)

type assetUpdate struct {
	collection string
	contract   string
	identifier string
	newOwner   string
	outLands   []string
	inLands    []string
	operations []primitive.ObjectID
}

const (
	filterTxLogsInfoColAssetTransfers = "CollectionAssetsTransfers"
	filterTxLogsInfoColAssetInter     = "CollectionAssetsInter"
	filterTxLogsInfoColAssetAll       = "CollectionAssetsAll"
	filterTxLogsInfoAllAssetTransfers = "AllAssetsTransfers"
	filterTxLogsInfoMoneyTransfers    = "MoneyTransfers"
)

func safeGetAssetForParser(collection, contract, assetId string, allAssets []*Asset) *Asset {
	index := slices.IndexFunc(allAssets, func(asset *Asset) bool {
		return asset.Collection == collection && asset.Contract == contract && asset.AssetId == assetId
	})
	if index < 0 {
		return nil
	} else {
		return allAssets[index]
	}
}

func getCurrenciesAddresses(currencies map[string]*collections.Currency) []string {
	addresses := make([]string, 0)
	for _, currency := range currencies {
		if !slices.Contains(addresses, strings.ToLower(currency.Contract)) {
			addresses = append(addresses, strings.ToLower(currency.Contract))
		}
	}
	return addresses
}

func filterManyTransferLogsForOneAsset(txLogsInfo []*TransactionLogInfo) (filtered []*TransactionLogInfo) {
	assets := make([]string, 0)
	for _, logInfo := range txLogsInfo {
		if logInfo.Asset != "" {
			if !slices.Contains(assets, logInfo.Asset) {
				assets = append(assets, logInfo.Asset)
			}
		}
	}
	filtered = make([]*TransactionLogInfo, 0)
	for _, assetId := range assets {
		transfersLogsOfAssets := helpers.ArrayFilter(txLogsInfo, func(t *TransactionLogInfo) bool {
			return t.Asset != "" && t.Asset == assetId
		})
		if len(transfersLogsOfAssets) > 0 {
			if len(transfersLogsOfAssets) > 1 {
				senders := helpers.ArrayMap(transfersLogsOfAssets, func(t *TransactionLogInfo) (bool, string) {
					if t.From != "" {
						return true, t.From
					} else {
						return false, ""
					}
				}, true, "")
				receivers := helpers.ArrayMap(transfersLogsOfAssets, func(t *TransactionLogInfo) (bool, string) {
					if t.To != "" {
						return true, t.To
					} else {
						return false, ""
					}
				}, false, "")
				fSenders := helpers.ArrayFilter(senders, func(s string) bool {
					return !slices.Contains(receivers, s)
				})
				fReceivers := helpers.ArrayFilter(receivers, func(s string) bool {
					return !slices.Contains(senders, s)
				})
				logInfo := transfersLogsOfAssets[0]
				if len(fSenders) > 0 {
					logInfo.From = fSenders[0]
				} else {
					logInfo.From = senders[0]
				}
				if len(fReceivers) > 0 {
					logInfo.To = fReceivers[0]
				} else {
					logInfo.To = receivers[0]
				}
				filtered = append(filtered, logInfo)
			} else {
				filtered = append(filtered, transfersLogsOfAssets[0])
			}
		}
	}
	return filtered
}

func filterTransactionLogsInfo(txLogsInfos []*TransactionLogInfo, filterName string) (filtered []*TransactionLogInfo) {
	filtered = make([]*TransactionLogInfo, 0)
	if filterName == filterTxLogsInfoColAssetTransfers {
		for _, info := range txLogsInfos {
			if info.EventName == os.Getenv("ETH_TRANSFER_LOG_ASSET") && info.IsCollectionAsset {
				filtered = append(filtered, info)
			}
		}
	} else if filterName == filterTxLogsInfoColAssetInter {
		for _, info := range txLogsInfos {
			if info.EventName != os.Getenv("ETH_TRANSFER_LOG_ASSET") && info.IsCollectionAsset {
				filtered = append(filtered, info)
			}
		}
	} else if filterName == filterTxLogsInfoColAssetAll {
		for _, info := range txLogsInfos {
			if info.IsCollectionAsset {
				filtered = append(filtered, info)
			}
		}
	} else if filterName == filterTxLogsInfoAllAssetTransfers {
		for _, info := range txLogsInfos {
			if info.EventName == os.Getenv("ETH_TRANSFER_LOG_ASSET") {
				filtered = append(filtered, info)
			}
		}
	} else if filterName == filterTxLogsInfoMoneyTransfers {
		for _, info := range txLogsInfos {
			if info.EventName == os.Getenv("ETH_TRANSFER_LOG_MONEY") {
				filtered = append(filtered, info)
			}
		}
	}
	if filterName == filterTxLogsInfoColAssetTransfers || filterName == filterTxLogsInfoAllAssetTransfers {
		filtered = filterManyTransferLogsForOneAsset(filtered)
	}
	return filtered
}

func extractLogInfosForTxLogItem(txLog *transactions_infos.TransactionLog, cltInfo *collections.CollectionInfo, currencies map[string]*collections.Currency) *TransactionLogInfo {
	topics := txLog.Topics
	address := txLog.Address
	blockchain := txLog.Blockchain
	data := txLog.Data
	eventHex := topics[0]
	var logInfo *TransactionLogInfo
	if cltInfo.HasAsset(address, blockchain) {
		logTopic := cltInfo.GetLogTopic(address, blockchain, eventHex)
		if logTopic != nil {
			logInfo = &TransactionLogInfo{}
			logInfo.EventName = logTopic.Name
			logInfo.IsCollectionAsset = true
			if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
				if logTopic.Name == os.Getenv("ETH_TRANSFER_LOG_ASSET") {
					logInfo.From = helpers.HexRemoveLeadingZeros(topics[1])
					logInfo.To = helpers.HexRemoveLeadingZeros(topics[2])
					logInfo.Asset, _ = helpers.HexConvertToString(topics[3])
				} else if logTopic.Name == os.Getenv("ETH_TRANSFER_LOG_DCL_ADD_LAND") {
					logInfo.Estate, _ = helpers.HexConvertToString(topics[1])
					logInfo.Land, _ = helpers.HexConvertToString(topics[2])
				} else if logTopic.Name == os.Getenv("ETH_TRANSFER_LOG_DCL_RMV_LAND") {
					logInfo.Estate, _ = helpers.HexConvertToString(topics[1])
					logInfo.Land, _ = helpers.HexConvertToString(topics[2])
					logInfo.To = helpers.HexRemoveLeadingZeros(topics[3])
				}
			}
		}
	} else {
		if eventHex == os.Getenv("ETH_TRANSFER_LOG_HEX") {
			currenciesAddresses := getCurrenciesAddresses(currencies)
			isMoney := len(topics) == 3 && data != "" && slices.Contains(currenciesAddresses, strings.ToLower(address))
			if isMoney {
				logInfo = &TransactionLogInfo{}
				logInfo.EventName = os.Getenv("ETH_TRANSFER_LOG_MONEY")
				logInfo.From = helpers.HexRemoveLeadingZeros(topics[1])
				logInfo.To = helpers.HexRemoveLeadingZeros(topics[2])
				logInfo.Amount, _ = helpers.HexConvertToString(data)
			} else if len(topics) == 4 || (len(topics) == 3 && data != "") {
				logInfo = &TransactionLogInfo{}
				logInfo.EventName = os.Getenv("ETH_TRANSFER_LOG_ASSET")
				logInfo.From = helpers.HexRemoveLeadingZeros(topics[1])
				logInfo.To = helpers.HexRemoveLeadingZeros(topics[2])
				logInfo.IsCollectionAsset = false
				if len(topics) == 4 {
					logInfo.Asset, _ = helpers.HexConvertToString(topics[3])
				} else {
					logInfo.Asset, _ = helpers.HexConvertToString(data)
				}
			}
		}
	}
	if logInfo != nil {
		logInfo.TransactionLog = txLog
	}
	return logInfo
}

func extractLogInfos(txLogs []*transactions_infos.TransactionLog, cltInfo *collections.CollectionInfo, currencies map[string]*collections.Currency) []*TransactionLogInfo {
	logInfos := make([]*TransactionLogInfo, 0)
	for _, txLog := range txLogs {
		logInfo := extractLogInfosForTxLogItem(txLog, cltInfo, currencies)
		if logInfo != nil {
			logInfos = append(logInfos, logInfo)
		}
	}
	return logInfos
}

func writeIDSInTransferLogs(txLogsInfo []*TransactionLogInfo) {
	for _, logInfo := range txLogsInfo {
		logInfo.TransactionLog.ID = primitive.NewObjectID()
	}
}

func getTransactionLogInfoReceivers(txLogsInfo []*TransactionLogInfo) []string {
	recipients := make([]string, 0)
	for _, logInfo := range txLogsInfo {
		if !slices.Contains(recipients, strings.ToLower(logInfo.To)) {
			recipients = append(recipients, strings.ToLower(logInfo.To))
		}
	}
	return recipients
}

func getNumberOfAssetsTransacted(txLogsInfo []*TransactionLogInfo, assetsReceivers []string) int {
	assets := make([]string, 0)
	filtered := filterTransactionLogsInfo(txLogsInfo, filterTxLogsInfoAllAssetTransfers)
	for _, logInfo := range filtered {
		if logInfo.Asset != "" {
			if !slices.Contains(assets, logInfo.Asset) && slices.Contains(assetsReceivers, logInfo.To) {
				assets = append(assets, logInfo.Asset)
			}
		}
	}
	return len(assets)
}

func getTransferMoneyLogsOnAssetsReceivers(txLogsInfo []*TransactionLogInfo, assetsReceivers []string, currencies map[string]*collections.Currency) []*TransactionLogInfo {
	currenciesAddresses := getCurrenciesAddresses(currencies)
	transferMoneyLogsInfos := filterTransactionLogsInfo(txLogsInfo, filterTxLogsInfoMoneyTransfers)
	filteredLogsInfos := make([]*TransactionLogInfo, 0)
	for _, logInfo := range transferMoneyLogsInfos {
		if slices.Contains(currenciesAddresses, strings.ToLower(logInfo.TransactionLog.Address)) && (slices.Contains(assetsReceivers, strings.ToLower(logInfo.From)) || slices.Contains(assetsReceivers, strings.ToLower(logInfo.To))) {
			filteredLogsInfos = append(filteredLogsInfos, logInfo)
		}
	}
	return filteredLogsInfos
}
