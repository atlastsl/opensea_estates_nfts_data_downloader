package operations

import (
	"decentraland_data_downloader/modules/core/metaverses"
	"go.mongodb.org/mongo-driver/mongo"
)

func getAdditionalData(metaverse metaverses.MetaverseName, dbInstance *mongo.Database) (map[string]any, error) {
	cltInfo, err := metaverses.GetMetaverseInfoInDatabase(metaverse, dbInstance)
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
	return map[string]any{"cltInfo": cltInfo, "currencies": currencies, "allPrices": currenciesPrices}, nil
}
