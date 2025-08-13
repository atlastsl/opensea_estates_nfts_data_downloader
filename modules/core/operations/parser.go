package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
)

func formatAssetsUpdatesList(updates []*assetUpdate, mtvInfo *metaverses.MetaverseInfo, allAssets []*metaverses.MetaverseAsset) ([]*assetUpdateFormatted, error) {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return nil, err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	var wLocker sync.RWMutex
	fAllUpdates := make([]*assetUpdateFormatted, 0)
	allErrors := make([]error, 0)

	var wg = &sync.WaitGroup{}
	for _, updateItem := range updates {
		wg.Add(1)
		go func() {
			metadataListI := make([]*assetUpdateFormatted, 0)
			var err error
			if metaverses.MetaverseName(mtvInfo.Name) == metaverses.MetaverseDcl {
				metadataListI, err = dclConvertAssetUpdateToMetadataUpdates(updateItem, allAssets, mtvInfo, dbInstance)
			} else {
				err = invalidCollectionError
			}
			wLocker.Lock()
			if err != nil {
				allErrors = append(allErrors, err)
			} else if len(metadataListI) > 0 {
				fAllUpdates = append(fAllUpdates, metadataListI...)
			}
			wLocker.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	if len(allErrors) > 0 {
		return nil, allErrors[0]
	}

	//allMetadata := make([]*assetUpdateFormatted, 0)
	//for _, updateItem := range updates {
	//	metadataListI := make([]*assetUpdateFormatted, 0)
	//	if metaverses.MetaverseName(mtvInfo.Name) == metaverses.MetaverseDcl {
	//		metadataListI, err = dclConvertAssetUpdateToMetadataUpdates(updateItem, allAssets, mtvInfo, dbInstance)
	//	} else {
	//		err = invalidCollectionError
	//	}
	//	if err != nil {
	//		return nil, err
	//	} else if len(metadataListI) > 0 {
	//		allMetadata = append(allMetadata, metadataListI...)
	//	}
	//}

	return fAllUpdates, nil
}

func convertTxLogsToAssetUpdates(txLogsInfos []*TransactionLogInfo, mtvInfo *metaverses.MetaverseInfo) ([]*assetUpdate, error) {
	if metaverses.MetaverseName(mtvInfo.Name) == metaverses.MetaverseDcl {
		assetsUpdatesList := dclConvertTxLogsToAssetUpdates(txLogsInfos, mtvInfo)
		return assetsUpdatesList, nil
	} else {
		return nil, invalidCollectionError
	}
}

func parseTransaction(txFull *TransactionFull, params map[string]any) ([]*Operation, error) {
	cltInfo := params["cltInfo"].(*metaverses.MetaverseInfo)
	currencies := params["currencies"].(map[string]*metaverses.Currency)
	allPrices := params["allPrices"].(map[string][]*metaverses.CurrencyPrice)

	txLogsInfos := extractLogInfos(txFull.Logs, cltInfo, currencies)
	assetsUpdatesList, err := convertTxLogsToAssetUpdates(txLogsInfos, cltInfo)
	if err != nil {
		print("ERROR FROM convertTxLogsToAssetUpdates")
		return nil, err
	}

	allAssets, err := findAllAssets(cltInfo, txLogsInfos)
	if err != nil {
		print("ERROR FROM findAllAssets")
		return nil, err
	}

	assetsFUpdatesList, err := formatAssetsUpdatesList(assetsUpdatesList, cltInfo, allAssets)
	if err != nil {
		print("ERROR FROM convertAssetsUpdatesListAsMetadata")
		return nil, err
	}

	operations, err := convertTransactionInfoToOperations(txFull, txLogsInfos, cltInfo, currencies, allPrices, allAssets, assetsFUpdatesList)
	if err != nil {
		print("ERROR FROM convertTransactionInfoToOperations")
		return nil, err
	}

	return operations, nil
}

func parseTransactions(transactions []*TransactionFull, params map[string]any, dbInstance *mongo.Database, _ *sync.WaitGroup) error {
	operations := make([]*Operation, 0)
	allErrors := make([]error, 0)
	var aWg = &sync.WaitGroup{}
	var dataLocker = sync.RWMutex{}

	for _, transaction := range transactions {
		aWg.Add(1)
		go func() {
			defer aWg.Done()
			tOperations, err := parseTransaction(transaction, params)
			dataLocker.Lock()
			if err != nil {
				allErrors = append(allErrors, err)
			} else {
				operations = append(operations, tOperations...)
			}
			dataLocker.Unlock()
		}()
	}
	aWg.Wait()

	if len(allErrors) > 0 {
		return allErrors[0]
	}

	err := saveOperationsInDatabase(operations, dbInstance)
	return err

	//wg.Add(1)
	//go func() {
	//	_ = saveOperationsInDatabase(operations, dbInstance)
	//	wg.Done()
	//}()

	//operations := make([]*Operation, 0)
	//
	//for _, transaction := range transactions {
	//	tOperations, err := parseTransaction(transaction, params)
	//	if err != nil {
	//		return err
	//	}
	//	operations = append(operations, tOperations...)
	//}

	//return nil
}
