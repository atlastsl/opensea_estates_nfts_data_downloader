package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/movements"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
	"time"
)

func convertAssetsUpdatesListAsMetadata(cltInfo *collections.CollectionInfo, allAssets []*Asset, updates []*assetUpdate, blockTimestamp time.Time) ([]*AssetMetadata, error) {
	dbInstance, err := database.NewDatabaseConnection()
	defer database.CloseDatabaseConnection(dbInstance)
	if err != nil {
		return nil, err
	}

	var wLocker sync.RWMutex
	allMetadata := make([]*AssetMetadata, 0)
	allErrors := make([]error, 0)

	var wg = &sync.WaitGroup{}
	for _, updateItem := range updates {
		wg.Add(1)
		go func() {
			metadataListI := make([]*AssetMetadata, 0)
			var err error
			if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
				metadataListI, err = dclConvertAssetUpdateToMetadataUpdates(updateItem, allAssets, blockTimestamp, cltInfo, dbInstance)
			} else {
				err = invalidCollectionError
			}
			wLocker.Lock()
			if err != nil {
				allErrors = append(allErrors, err)
			} else if len(metadataListI) > 0 {
				allMetadata = append(allMetadata, metadataListI...)
			}
			wLocker.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	if len(allErrors) > 0 {
		return nil, allErrors[0]
	}

	return allMetadata, nil
}

func convertTxLogsToAssetUpdates(txLogsInfos []*TransactionLogInfo, cltInfo *collections.CollectionInfo) ([]*assetUpdate, error) {
	if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
		assetsUpdatesList := dclConvertTxLogsToAssetUpdates(txLogsInfos, cltInfo)
		return assetsUpdatesList, nil
	} else {
		return nil, invalidCollectionError
	}
}

func parseTransaction(txFull *TransactionFull, params map[string]any) ([]*Operation, []*AssetMetadata, error) {
	cltInfo := params["cltInfo"].(*collections.CollectionInfo)
	currencies := params["currencies"].(map[string]*collections.Currency)
	allDistances := params["allDistances"].([]*tiles_distances.MapTileMacroDistance)
	allPrices := params["allPrices"].(map[string][]*movements.CurrencyPrice)

	txLogsInfos := extractLogInfos(txFull.Logs, cltInfo, currencies)
	assetsUpdatesList, err := convertTxLogsToAssetUpdates(txLogsInfos, cltInfo)
	if err != nil {
		return nil, nil, err
	}

	allAssets, err := findAllAssets(cltInfo, txLogsInfos, allDistances)
	if err != nil {
		return nil, nil, err
	}

	assetsMetadataList, err := convertAssetsUpdatesListAsMetadata(cltInfo, allAssets, assetsUpdatesList, txFull.Transaction.BlockTimestamp)
	if err != nil {
		return nil, nil, err
	}

	operations, err := convertTransactionInfoToOperations(txFull, txLogsInfos, cltInfo, currencies, allPrices, allAssets)
	if err != nil {
		return nil, nil, err
	}

	return operations, assetsMetadataList, nil
}

func saveOperationsAndMetadata(operations []*Operation, metadataList []*AssetMetadata, dbInstance *mongo.Database) error {
	err := saveAssetMetadataInDatabase(metadataList, dbInstance)
	if err != nil {
		return err
	}
	err = saveOperationsInDatabase(operations, dbInstance)
	if err != nil {
		return err
	}

	return nil
}

func parseTransactions(transactions []*TransactionFull, params map[string]any, dbInstance *mongo.Database, wg *sync.WaitGroup) error {
	metadataList := make([]*AssetMetadata, 0)
	operations := make([]*Operation, 0)
	allErrors := make([]error, 0)
	var aWg = &sync.WaitGroup{}
	var dataLocker = sync.RWMutex{}

	for _, transaction := range transactions {
		aWg.Add(1)
		go func() {
			defer aWg.Done()
			tOperations, tMetadataList, err := parseTransaction(transaction, params)
			dataLocker.Lock()
			if err != nil {
				allErrors = append(allErrors, err)
			} else {
				operations = append(operations, tOperations...)
				metadataList = append(metadataList, tMetadataList...)
			}
			dataLocker.Unlock()
		}()
	}
	aWg.Wait()

	if len(allErrors) > 0 {
		return allErrors[0]
	}

	wg.Add(1)
	go func() {
		_ = saveOperationsAndMetadata(operations, metadataList, dbInstance)
		wg.Done()
	}()

	return nil
}
