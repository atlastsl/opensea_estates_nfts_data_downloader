package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"sync"
)

func getAssetIdentifierFromLogs(mtvInfo *metaverses.MetaverseInfo, logsInfo []*TransactionLogInfo) []map[string]string {
	result := make([]map[string]string, 0)
	if metaverses.MetaverseName(mtvInfo.Name) == metaverses.MetaverseDcl {
		result = dclGetAssetIdentifierFromLogs(mtvInfo, logsInfo)
	}
	return result
}

func findAllAssets(mtvInfo *metaverses.MetaverseInfo, txLogsInfos []*TransactionLogInfo) ([]*metaverses.MetaverseAsset, error) {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return nil, err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	assets := make([]*metaverses.MetaverseAsset, 0)
	allErrors := make([]error, 0)
	wg := &sync.WaitGroup{}
	dataLocker := &sync.RWMutex{}

	colAllLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetAll)
	assetIds := getAssetIdentifierFromLogs(mtvInfo, colAllLogsInfo)

	for _, assetIdItem := range assetIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			contract, assetId := assetIdItem["contract"], assetIdItem["asset_id"]
			asset, e0 := getAssetFromDatabase(mtvInfo.Name, contract, assetId, dbInstance)
			dataLocker.Lock()
			if e0 != nil {
				allErrors = append(allErrors, e0)
			} else {
				assets = append(assets, asset)
			}
			dataLocker.Unlock()
		}()
	}
	wg.Wait()

	if len(allErrors) > 0 {
		return nil, allErrors[0]
	} else {
		return assets, nil
	}

	//assets := make([]*metaverses.MetaverseAsset, 0)
	//
	//colAllLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetAll)
	//assetIds := getAssetIdentifierFromLogs(mtvInfo, colAllLogsInfo)
	//
	//for _, assetIdItem := range assetIds {
	//	contract, assetId := assetIdItem["contract"], assetIdItem["asset_id"]
	//	asset, e0 := getAssetFromDatabase(mtvInfo.Name, contract, assetId, dbInstance)
	//	if e0 != nil {
	//		return nil, e0
	//	}
	//	assets = append(assets, asset)
	//}
	//
	//transfersLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetTransfers)
	//
	//for _, logInfo := range transfersLogsInfo {
	//	asset, e0 := getAssetFromDatabase(mtvInfo.Name, logInfo.TransactionLog.Address, logInfo.Asset, dbInstance)
	//	if e0 != nil {
	//		return nil, e0
	//	}
	//	assets = append(assets, asset)
	//}

	return assets, nil
}
