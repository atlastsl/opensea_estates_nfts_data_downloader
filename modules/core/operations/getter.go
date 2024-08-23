package operations

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"go.mongodb.org/mongo-driver/mongo"
)

func getAdditionalData(collection collections.Collection, dbInstance *mongo.Database) (map[string]any, error) {
	cltInfo, err := getNftCollectionInfo(collection, dbInstance)
	if err != nil {
		return nil, err
	}
	currencies, err := getCurrencies(dbInstance)
	if err != nil {
		return nil, err
	}
	currenciesPrices, err := getCurrencyPrices(dbInstance)
	if err != nil {
		return nil, err
	}
	tilesDistances := make([]*tiles_distances.MapTileMacroDistance, 0)
	if collection == collections.CollectionDcl {
		landInfo := cltInfo.GetAsset("land")
		tilesDistances, err = fetchTileMacroDistances(collection, landInfo.Contract, dbInstance)
		if err != nil {
			return nil, err
		}
	}
	return map[string]any{"cltInfo": cltInfo, "currencies": currencies, "allPrices": currenciesPrices, "allDistances": tilesDistances}, nil
}
