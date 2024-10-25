package assets

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"slices"
)

func fetchTileMacroDistances(collection collections.Collection, contract string, dbInstance *mongo.Database) ([]*tiles_distances.MapTileMacroDistance, error) {
	tmDistancesCol := database.CollectionInstance(dbInstance, &tiles_distances.MapTileMacroDistance{})
	regexPattern := fmt.Sprintf("%s|%s|", string(collection), contract)
	cursor, err := tmDistancesCol.Find(context.Background(), bson.M{"tile_slug": bson.M{"$regex": primitive.Regex{Pattern: regexPattern, Options: "i"}}})
	if err != nil {
		return nil, err
	}
	var distances []*tiles_distances.MapTileMacroDistance
	err = cursor.All(context.Background(), &distances)
	if err != nil {
		return nil, err
	}
	err = cursor.Close(context.Background())
	if err != nil {
		return nil, err
	}
	return distances, nil
}

func saveEstateAssetInDatabase(assetsInfos []*EstateAssetAll, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &EstateAsset{})

	operations := make([]mongo.WriteModel, len(assetsInfos))
	for i, assetInfo := range assetsInfos {
		var filterPayload = bson.M{"identifier": assetInfo.asset.Identifier, "collection": assetInfo.asset.Collection, "contract": assetInfo.asset.Contract}
		operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(assetInfo.asset).SetUpsert(true)
	}
	_, err := dbCollection.BulkWrite(context.Background(), operations)

	return err
}

func writeAssetEstateIDInMetadata(assetsInfos []*EstateAssetAll, dbInstance *mongo.Database) ([]*EstateAssetMetadata, error) {
	dbCollection := database.CollectionInstance(dbInstance, &EstateAsset{})

	payloads := bson.A{}
	for _, assetInfo := range assetsInfos {
		var filterPayload = bson.M{"identifier": assetInfo.asset.Identifier, "collection": assetInfo.asset.Collection, "contract": assetInfo.asset.Contract}
		payloads = append(payloads, filterPayload)
	}
	filterPayload := bson.M{"$or": payloads}

	cursor, err := dbCollection.Find(context.Background(), filterPayload)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())

	var result []EstateAsset
	err = cursor.All(context.Background(), &result)
	if err != nil {
		return nil, err
	}

	metadataList := make([]*EstateAssetMetadata, 0)
	for _, assetInfo := range assetsInfos {
		dbAssetIdx := slices.IndexFunc(result, func(asset EstateAsset) bool {
			return assetInfo.asset.Identifier == asset.Identifier && assetInfo.asset.Contract == asset.Contract && assetInfo.asset.Collection == asset.Collection
		})
		if dbAssetIdx >= 0 {
			for _, metadata := range assetInfo.assetMetadata {
				metadata.EstateAssetRef = result[dbAssetIdx].ID
				metadataList = append(metadataList, metadata)
			}
		}
	}

	return metadataList, nil
}

func saveEstateMetadataInDatabase(assetMetadata []*EstateAssetMetadata, dbInstance *mongo.Database) error {
	if assetMetadata != nil && len(assetMetadata) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EstateAssetMetadata{})
		operations := make([]mongo.WriteModel, len(assetMetadata))
		for i, metadata := range assetMetadata {
			var filterPayload bson.M
			if !metadata.MacroRef.IsZero() {
				filterPayload = bson.M{"macro": metadata.MacroRef, "estate_asset": metadata.EstateAssetRef}
			} else {
				filterPayload = bson.M{"estate_asset": metadata.EstateAssetRef}
			}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(metadata).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return errors.New("metadata empty list")
}

func saveEstateAssetInfoInDatabase(assetsInfos []*EstateAssetAll) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)
	if assetsInfos != nil && len(assetsInfos) > 0 {
		err = saveEstateAssetInDatabase(assetsInfos, dbInstance)
		if err != nil {
			println(err.Error())
			return err
		}
		metadataList, err2 := writeAssetEstateIDInMetadata(assetsInfos, dbInstance)
		if err2 != nil {
			println(err2.Error())
			return err2
		}
		err = saveEstateMetadataInDatabase(metadataList, dbInstance)
		if err != nil {
			println(err.Error())
			return err
		}
	}

	return nil
}
