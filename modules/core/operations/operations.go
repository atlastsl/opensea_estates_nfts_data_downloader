package operations

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"math/big"
	"slices"
	"time"
)

const minAmtValue = 1e-6

func getCurrencyPrice(currency string, date time.Time, allPrices map[string][]*collections.CurrencyPrice) (price float64) {
	price = 0.0
	filteredPrices, hasCp := allPrices[currency]
	if hasCp && filteredPrices != nil && len(filteredPrices) > 0 {
		if date.UnixMilli() < filteredPrices[0].Start.UnixMilli() {
			price = filteredPrices[0].Open
		} else if date.UnixMilli() >= filteredPrices[len(filteredPrices)-1].End.UnixMilli() {
			price = filteredPrices[len(filteredPrices)-1].Close
		} else {
			bestPriceInstance := new(collections.CurrencyPrice)
			for _, priceItem := range filteredPrices {
				if priceItem.Start.UnixMilli() <= date.UnixMilli() && date.UnixMilli() < priceItem.End.UnixMilli() {
					bestPriceInstance = priceItem
					break
				}
			}
			if bestPriceInstance != nil {
				openP := new(big.Float).SetFloat64(bestPriceInstance.Open)
				closeP := new(big.Float).SetFloat64(bestPriceInstance.Close)
				highP := new(big.Float).SetFloat64(bestPriceInstance.High)
				lowP := new(big.Float).SetFloat64(bestPriceInstance.Low)
				temp := new(big.Float).Add(openP, closeP)
				temp = temp.Add(temp, highP)
				temp = temp.Add(temp, lowP)
				temp = temp.Quo(temp, new(big.Float).SetFloat64(4.0))
				price, _ = temp.Float64()
			}
		}
	}
	return price
}

func getFullCurrencyPrice(currency string, date time.Time, allPrices map[string][]*collections.CurrencyPrice) *collections.CurrencyPrice {
	filteredPrices, hasCp := allPrices[currency]
	if hasCp && filteredPrices != nil && len(filteredPrices) > 0 {
		if date.UnixMilli() < filteredPrices[0].Start.UnixMilli() {
			return filteredPrices[0]
		} else if date.UnixMilli() >= filteredPrices[len(filteredPrices)-1].End.UnixMilli() {
			return filteredPrices[len(filteredPrices)-1]
		} else {
			bestPriceInstance := new(collections.CurrencyPrice)
			for _, priceItem := range filteredPrices {
				if priceItem.Start.UnixMilli() <= date.UnixMilli() && date.UnixMilli() < priceItem.End.UnixMilli() {
					bestPriceInstance = priceItem
					break
				}
			}
			if bestPriceInstance != nil {
				return bestPriceInstance
			}
		}
	}
	return nil
}

func getCurrency(currencyAddress, blockchain string, currencies map[string]*collections.Currency) (*collections.Currency, bool) {
	currency, ccyExists := currencies[currencyAddress]
	if !ccyExists {
		for _, ccy := range currencies {
			if ccy.Blockchain == blockchain && ccy.MainCurrency {
				currency = ccy
				ccyExists = true
				break
			}
		}
	}
	return currency, ccyExists
}

func getTransferMoneyOperationValues(transferMoneyLogsInfo []*TransactionLogInfo, assetsReceivers []string, date time.Time, nbAssetsTransacted int, currencies map[string]*collections.Currency, allPrices map[string][]*collections.CurrencyPrice) []OperationValue {
	currenciesAddresses := make([]string, 0)
	for _, logInfo := range transferMoneyLogsInfo {
		if !slices.Contains(currenciesAddresses, logInfo.TransactionLog.Address) {
			currenciesAddresses = append(currenciesAddresses, logInfo.TransactionLog.Address)
		}
	}
	opValues := make([]OperationValue, 0)
	for _, currencyAddress := range currenciesAddresses {
		currency, ccyExists := getCurrency(currencyAddress, "-", currencies)
		if ccyExists {
			price := getCurrencyPrice(currency.Symbols, date, allPrices)
			ccyLogs := helpers.ArrayFilter(transferMoneyLogsInfo, func(logInfo *TransactionLogInfo) bool {
				return logInfo.TransactionLog.Address == currencyAddress && logInfo.Amount != ""
			})
			bgCcyOpValue := big.NewFloat(0.0)
			if len(ccyLogs) > 0 {
				for _, logInfo := range ccyLogs {
					bgAmt := new(big.Float)
					bgAmt, _ = bgAmt.SetString(logInfo.Amount)
					if slices.Contains(assetsReceivers, logInfo.From) {
						bgCcyOpValue.Add(bgCcyOpValue, bgAmt)
					} else {
						bgCcyOpValue.Sub(bgCcyOpValue, bgAmt)
					}
				}
				if bgCcyOpValue.Cmp(big.NewFloat(0.0)) < 0 {
					bgCcyOpValue.SetFloat64(0.0)
				}
			}
			if bgCcyOpValue.Cmp(big.NewFloat(0.0)) > 0 {
				decimals := new(big.Int)
				decimals.Exp(big.NewInt(10), big.NewInt(currency.Decimals), nil)
				bgCcyOpValue.Quo(bgCcyOpValue, new(big.Float).SetInt(decimals))
				if bgCcyOpValue.Cmp(big.NewFloat(minAmtValue)) > 0 {
					if nbAssetsTransacted > 1 {
						bgCcyOpValue.Quo(bgCcyOpValue, new(big.Float).SetInt64(int64(nbAssetsTransacted)))
					}
					bgCcyOpValueUsd := new(big.Float).SetFloat64(0.0)
					if price > 0 {
						bgCcyOpValueUsd.Mul(bgCcyOpValue, new(big.Float).SetFloat64(price))
					}
					value, _ := bgCcyOpValue.Float64()
					valueUsd, _ := bgCcyOpValueUsd.Float64()
					opValues = append(opValues, OperationValue{Value: value, Currency: currency.Symbols, CurrencyPrice: price, ValueUsd: valueUsd})
				}
			}
		}
	}
	return opValues
}

func getTransactionValueOperationValue(txInfo *transactions_infos.TransactionInfo, nbAssetsTransacted int, currencies map[string]*collections.Currency, allPrices map[string][]*collections.CurrencyPrice) *OperationValue {
	currency, ccyExists := getCurrency("-", txInfo.Blockchain, currencies)
	if ccyExists {
		price := getCurrencyPrice(currency.Symbols, txInfo.BlockTimestamp, allPrices)
		bgCcyOpValue := new(big.Float)
		bgCcyOpValue, _ = bgCcyOpValue.SetString(txInfo.Value)
		if bgCcyOpValue.Cmp(big.NewFloat(0)) == 1 {
			decimals := new(big.Int)
			decimals.Exp(big.NewInt(10), big.NewInt(currency.Decimals), nil)
			bgCcyOpValue.Quo(bgCcyOpValue, new(big.Float).SetInt(decimals))
			if nbAssetsTransacted > 1 {
				bgCcyOpValue.Quo(bgCcyOpValue, new(big.Float).SetInt64(int64(nbAssetsTransacted)))
			}
			bgCcyOpValueUsd := new(big.Float).SetFloat64(0.0)
			if price > 0 {
				bgCcyOpValueUsd.Mul(bgCcyOpValue, new(big.Float).SetFloat64(price))
			}
			value, _ := bgCcyOpValue.Float64()
			valueUsd, _ := bgCcyOpValueUsd.Float64()
			return &OperationValue{Value: value, Currency: currency.Symbols, CurrencyPrice: price, ValueUsd: valueUsd}
		}
	}
	return nil
}

func getTransactionOperationValues(transaction *TransactionFull, transferMoneyLogsInfo []*TransactionLogInfo, assetsReceivers []string, nbAssetsTransacted int, currencies map[string]*collections.Currency, allPrices map[string][]*collections.CurrencyPrice) []OperationValue {
	opValues := make([]OperationValue, 0)
	trMoneyOpValues := getTransferMoneyOperationValues(transferMoneyLogsInfo, assetsReceivers, transaction.Transaction.BlockTimestamp, nbAssetsTransacted, currencies, allPrices)
	if len(trMoneyOpValues) > 0 {
		opValues = append(opValues, trMoneyOpValues...)
	}
	mainCcyOpValue := getTransactionValueOperationValue(transaction.Transaction, nbAssetsTransacted, currencies, allPrices)
	if mainCcyOpValue != nil {
		opValues = append(opValues, *mainCcyOpValue)
	}
	return opValues
}

func getTransactionFeesOperationValue(txInfo *transactions_infos.TransactionInfo, nbAssetsTransacted int, currencies map[string]*collections.Currency, allPrices map[string][]*collections.CurrencyPrice) []OperationValue {
	opValues := make([]OperationValue, 0)
	currency, ccyExists := getCurrency("-", txInfo.Blockchain, currencies)
	if ccyExists {
		price := getCurrencyPrice(currency.Symbols, txInfo.BlockTimestamp, allPrices)
		gasUsed, gasPrice := new(big.Float), new(big.Float)
		gasUsed, _ = gasUsed.SetString(txInfo.GasUsed)
		gasPrice, _ = gasPrice.SetString(txInfo.GasPrice)
		gasValue := new(big.Float).Mul(gasUsed, gasPrice)
		if gasValue.Cmp(big.NewFloat(0)) == 1 {
			decimals := new(big.Int)
			decimals.Exp(big.NewInt(10), big.NewInt(currency.Decimals), nil)
			gasValue.Quo(gasValue, new(big.Float).SetInt(decimals))
			if nbAssetsTransacted > 1 {
				gasValue.Quo(gasValue, new(big.Float).SetInt64(int64(nbAssetsTransacted)))
			}
			bgGasValueUsd := new(big.Float).SetFloat64(0.0)
			if price > 0 {
				bgGasValueUsd.Mul(gasValue, new(big.Float).SetFloat64(price))
			}
			value, _ := gasValue.Float64()
			valueUsd, _ := bgGasValueUsd.Float64()
			opValues = append(opValues, OperationValue{Value: value, Currency: currency.Symbols, CurrencyPrice: price, ValueUsd: valueUsd})
		}
	}
	return opValues
}

func getTransactionMarketInfo(txInfo *transactions_infos.TransactionInfo, cltInfo *collections.CollectionInfo, allPrices map[string][]*collections.CurrencyPrice) *MarketDataInfo {
	currencySymbols := ""
	switch cltInfo.Name {
	case "decentraland":
		currencySymbols = "MANA"
		break
	case "somnium-space":
		currencySymbols = "CUBE"
		break
	case "crypto-voxels":
		currencySymbols = ""
		break
	case "the-sandbox":
		currencySymbols = "SAND"
		break
	}
	mdi := &MarketDataInfo{}
	if currencySymbols != "" {
		yesterday := time.UnixMilli(txInfo.BlockTimestamp.UnixMilli()).AddDate(0, 0, -1)
		bYesterday := time.UnixMilli(yesterday.UnixMilli()).AddDate(0, 0, -1)
		ytdPrice := getFullCurrencyPrice(currencySymbols, yesterday, allPrices)
		bytdPrice := getFullCurrencyPrice(currencySymbols, bYesterday, allPrices)
		mdi.Price = getCurrencyPrice(currencySymbols, txInfo.BlockTimestamp, allPrices)
		if ytdPrice != nil {
			tmp := new(big.Float).SetFloat64(0.0)
			if bytdPrice != nil {
				tmp = tmp.Sub(new(big.Float).SetFloat64(ytdPrice.Close), new(big.Float).SetFloat64(bytdPrice.Close))
				tmp = new(big.Float).Quo(tmp, new(big.Float).SetFloat64(bytdPrice.Close))
			} else {
				tmp = tmp.Sub(new(big.Float).SetFloat64(ytdPrice.Close), new(big.Float).SetFloat64(ytdPrice.Open))
				tmp = new(big.Float).Quo(tmp, new(big.Float).SetFloat64(ytdPrice.Open))
			}
			mdi.Change24h, _ = tmp.Float64()
			mdi.Currency = currencySymbols
			mdi.MarketCap = ytdPrice.MarketCap
			mdi.Volume24h = ytdPrice.Volume
		}
	}
	return mdi
}

func getOperationTypes(amount []OperationValue, sender string) (string, string) {
	operationType, transactionType := "", ""
	if len(amount) > 0 {
		operationType = OperationTypeSale
	} else {
		operationType = OperationTypeFree
	}
	if sender == "0x" || sender == "0x0" {
		transactionType = TransactionTypeMint
	} else {
		transactionType = TransactionTypeTransfer
	}
	return operationType, transactionType
}

func convertTransferLogToOperation(transferLogInfo *TransactionLogInfo, transactionInfo *transactions_infos.TransactionInfo, amount, fees []OperationValue, marketDataInfo *MarketDataInfo, cltInfo *collections.CollectionInfo, allAssets []*Asset) (*Operation, error) {
	asset := safeGetAssetForParser(cltInfo.Name, transferLogInfo.TransactionLog.Address, transferLogInfo.Asset, allAssets)
	if asset == nil {
		return nil, assetNotFoundError
	}
	sender, recipient := transferLogInfo.From, transferLogInfo.To
	operationType, transactionType := getOperationTypes(amount, sender)
	operation := &Operation{}
	operation.ID = transferLogInfo.TransactionLog.ID
	operation.CreatedAt = time.Now()
	operation.UpdatedAt = time.Now()
	operation.Collection = cltInfo.Name
	operation.AssetRef = asset.ID
	operation.AssetContract = asset.Contract
	operation.AssetId = asset.AssetId
	operation.TransactionHash = transferLogInfo.TransactionLog.TransactionHash
	operation.OperationType = operationType
	operation.TransactionType = transactionType
	operation.Blockchain = transactionInfo.Blockchain
	operation.BlockNumber = int64(transactionInfo.BlockNumber)
	operation.BlockHash = transactionInfo.BlockHash
	operation.Date = transactionInfo.BlockTimestamp
	operation.Sender = sender
	operation.Recipient = recipient
	operation.Amount = amount
	operation.Fees = fees
	operation.MarketInfo = *marketDataInfo
	return operation, nil
}

func convertTransactionInfoToOperations(transaction *TransactionFull, txLogsInfos []*TransactionLogInfo, cltInfo *collections.CollectionInfo, currencies map[string]*collections.Currency, allPrices map[string][]*collections.CurrencyPrice, allAssets []*Asset) ([]*Operation, error) {
	colAssetsTransfersLogs := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetTransfers)
	assetsReceivers := getTransactionLogInfoReceivers(colAssetsTransfersLogs)
	nbAssetsTransacted := getNumberOfAssetsTransacted(txLogsInfos, assetsReceivers)

	transferMoneyLogs := getTransferMoneyLogsOnAssetsReceivers(txLogsInfos, assetsReceivers, currencies)
	amount := getTransactionOperationValues(transaction, transferMoneyLogs, assetsReceivers, nbAssetsTransacted, currencies, allPrices)
	fees := getTransactionFeesOperationValue(transaction.Transaction, nbAssetsTransacted, currencies, allPrices)
	marketDataInfo := getTransactionMarketInfo(transaction.Transaction, cltInfo, allPrices)

	operations := make([]*Operation, 0)
	for _, transferLog := range colAssetsTransfersLogs {
		operation, err := convertTransferLogToOperation(transferLog, transaction.Transaction, amount, fees, marketDataInfo, cltInfo, allAssets)
		if err != nil {
			return nil, err
		}
		operations = append(operations, operation)
	}
	return operations, nil
}
