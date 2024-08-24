package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
	"sync"
)

func findAssetById(cltInfo *collections.CollectionInfo, contractAddress string, assetId string, allDistances []*tiles_distances.MapTileMacroDistance, dbInstance *mongo.Database) (*Asset, error) {
	asset, err := getAssetFromDatabase(cltInfo.Name, contractAddress, assetId, dbInstance)
	if err != nil {
		return nil, err
	}
	if asset == nil {
		var assetMetadataList []*AssetMetadata
		if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
			asset, assetMetadataList, err = dclFetchAssetInfo(cltInfo, contractAddress, assetId, allDistances)
		} else {
			err = errors.New("invalid collection info")
		}
		if err != nil {
			return nil, err
		}
		err = saveAssetInDatabase(asset, dbInstance)
		if err != nil {
			return nil, err
		}
		err = saveAssetMetadataInDatabase(assetMetadataList, dbInstance)
		if err != nil {
			return nil, err
		}
	}
	return asset, nil
}

func checkIfAssetIsInList(assets []*Asset, contract, assetId string) bool {
	if assets == nil || len(assets) == 0 {
		return false
	}
	for _, asset := range assets {
		if asset.Contract == contract && asset.AssetId == assetId {
			return true
		}
	}
	return false
}

func getAssetIdentifierFromLogs(cltInfo *collections.CollectionInfo, logsInfo []*TransactionLogInfo) []map[string]string {
	result := make([]map[string]string, 0)
	if collections.Collection(cltInfo.Name) == collections.CollectionDcl {
		result = dclGetAssetIdentifierFromLogs(cltInfo, logsInfo)
	}
	return result
}

func findAllAssets(cltInfo *collections.CollectionInfo, txLogsInfos []*TransactionLogInfo, allDistances []*tiles_distances.MapTileMacroDistance) ([]*Asset, error) {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return nil, err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	assets := make([]*Asset, 0)
	allErrors := make([]error, 0)
	wg := &sync.WaitGroup{}
	dataLocker := &sync.RWMutex{}

	colAllLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetAll)
	assetIds := getAssetIdentifierFromLogs(cltInfo, colAllLogsInfo)

	for _, assetIdItem := range assetIds {
		wg.Add(1)
		go func() {
			defer wg.Done()
			contract, assetId := assetIdItem["contract"], assetIdItem["asset_id"]
			asset, e0 := findAssetById(cltInfo, contract, assetId, allDistances, dbInstance)
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

	/*assets := make([]*Asset, 0)

	transfersLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetTransfers)

	for _, logInfo := range transfersLogsInfo {
		asset, e0 := findAssetById(cltInfo, logInfo.TransactionLog.Address, logInfo.Asset, allDistances, dbInstance)
		if e0 != nil {
			return nil, e0
		}
		assets = append(assets, asset)
	}

	return assets, nil*/
}
