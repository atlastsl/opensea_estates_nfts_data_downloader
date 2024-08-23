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

	transfersLogsInfo := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetTransfers)

	for _, logInfo := range transfersLogsInfo {
		wg.Add(1)
		go func() {
			defer wg.Done()
			asset, e0 := findAssetById(cltInfo, logInfo.TransactionLog.Address, logInfo.Asset, allDistances, dbInstance)
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
}
