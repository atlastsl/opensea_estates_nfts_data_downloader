package metaverses

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

func saveMetaverseAssetsInDatabase(metaverseAssets []*MetaverseAsset, dbInstance *mongo.Database) error {
	if metaverseAssets != nil && len(metaverseAssets) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &MetaverseAsset{})

		bdOperations := make([]mongo.WriteModel, len(metaverseAssets))
		for i, mtvAsset := range metaverseAssets {
			var filterPayload = bson.M{"metaverse": mtvAsset.Metaverse, "blockchain": mtvAsset.Blockchain, "contract": mtvAsset.Contract, "asset_id": mtvAsset.AssetId}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(mtvAsset).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func saveMetaverseInfoInDatabase(metaverseInfo *MetaverseInfo) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	metaverseInfo.CreatedAt = time.Now()
	metaverseInfo.UpdatedAt = time.Now()
	dbCollection := database.CollectionInstance(dbInstance, &MetaverseInfo{})
	opts := &options.ReplaceOptions{}
	_, err = dbCollection.ReplaceOne(context.Background(), bson.M{"name": metaverseInfo.Name}, metaverseInfo, opts.SetUpsert(true))
	return err
}

func getMetaverseInfoInDatabase(name MetaverseName, dbInstance *mongo.Database) (*MetaverseInfo, error) {
	metaverseInfo := &MetaverseInfo{}
	dbCollection := database.CollectionInstance(dbInstance, metaverseInfo)
	payload := bson.M{"name": string(name)}
	err := dbCollection.FirstWithCtx(context.Background(), payload, metaverseInfo)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			metaverseInfo = nil
		}
	}
	return metaverseInfo, nil
}

func GetMetaverseInfoInDatabase(name MetaverseName, dbInstance *mongo.Database) (*MetaverseInfo, error) {
	return getMetaverseInfoInDatabase(name, dbInstance)
}
