package movements

import (
	"decentraland_data_downloader/modules/core/collections"
	"errors"
	"go.mongodb.org/mongo-driver/mongo"
)

func getAdditionalData(collection collections.Collection, dbInstance *mongo.Database) (map[string]any, error) {
	if collection == collections.CollectionDcl {
		assets, e0 := getAllEstateAssets(collection, dbInstance)
		if e0 != nil {
			return nil, e0
		}
		prices, e1 := getCurrencyPrices(collection, dbInstance)
		if e1 != nil {
			return nil, e1
		}
		data := map[string]any{
			"assets": assets,
			"prices": prices,
		}
		return data, nil
	}
	return nil, errors.New("invalid collection")
}
