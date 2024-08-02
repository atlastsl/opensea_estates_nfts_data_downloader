package assets

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func fetchTileFromDatabase(collection collections.Collection, contract, coords string, dbInstance *mongo.Database) (*tiles.MapTile, error) {
	tile := &tiles.MapTile{}
	tilesCollection := database.CollectionInstance(dbInstance, tile)
	err := tilesCollection.FirstWithCtx(context.Background(), bson.M{"contract": contract, "collection": string(collection), "coords": coords}, tile)
	if err != nil {
		return nil, err
	} else {
		return tile, nil
	}
}

func fetchTileMacroDistances(tile *tiles.MapTile, dbInstance *mongo.Database) ([]tiles_distances.MapTileMacroDistance, error) {
	tileSlug := tiles.GetTileSlug(tile)
	tmDistancesCol := database.CollectionInstance(dbInstance, &tiles_distances.MapTileMacroDistance{})
	cursor, err := tmDistancesCol.Find(context.Background(), bson.M{"tile_slug": tileSlug})
	if err != nil {
		return nil, err
	}
	var distances []tiles_distances.MapTileMacroDistance
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

func saveEstateAssetInDatabase(asset *EstateAsset, dbInstance *mongo.Database) (primitive.ObjectID, error) {
	dbCollection := database.CollectionInstance(dbInstance, &EstateAsset{})
	filterPayload := bson.M{"identifier": asset.Identifier, "collection": asset.Collection, "contract": asset.Contract}
	rpOptions := &options.FindOneAndReplaceOptions{}
	rpOptions.SetUpsert(true)
	res := dbCollection.FindOneAndReplace(context.Background(), filterPayload, asset, rpOptions)
	if res.Err() != nil {
		return primitive.ObjectID{}, res.Err()
	}
	updatedDoc := &EstateAsset{}
	err := res.Decode(updatedDoc)
	if err != nil {
		return primitive.ObjectID{}, err
	}
	return updatedDoc.ID, nil
}

func saveEstateMetadataInDatabase(assetMetadata []*EstateAssetMetadata, assetId primitive.ObjectID, dbInstance *mongo.Database) error {
	if assetMetadata != nil && len(assetMetadata) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EstateAssetMetadata{})
		operations := make([]mongo.WriteModel, len(assetMetadata))
		for i, metadata := range assetMetadata {
			metadata.EstateAssetRef = assetId
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
	return nil
}
