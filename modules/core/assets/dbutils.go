package assets

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func fetchTileFromDatabase(collection collections.Collection, contract, coords string, dbInstance *mongo.Database) (*tiles.MapTile, error) {
	tile := &tiles.MapTile{}
	tilesCollection := database.CollectionInstance(dbInstance, tile)
	err := tilesCollection.First(bson.M{"contract": contract, "collection": string(collection), "coords": coords}, tile)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			return nil, nil
		}
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

func saveEstateAssetInDatabase(asset *EstateAsset, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &EstateAsset{})
	existing := &EstateAsset{}
	err := dbCollection.First(bson.M{"identifier": asset.Identifier, "collection": asset.Collection, "contract": asset.Contract}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		found = false
	}
	if found {
		asset.ID = existing.ID
		err = dbCollection.Update(asset)
		if err != nil {
			return err
		}
	} else {
		err = dbCollection.Create(asset)
		if err != nil {
			return err
		}
	}
	return nil
}

func saveEstateMetadataInDatabase(assetMetadata []*EstateAssetMetadata, dbInstance *mongo.Database) error {
	if assetMetadata != nil && len(assetMetadata) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EstateAssetMetadata{})
		for _, metadata := range assetMetadata {
			existing := &EstateAssetMetadata{}
			var payload bson.M
			if !metadata.MacroRef.IsZero() {
				payload = bson.M{"macro": metadata.MacroRef, "estate_asset": metadata.EstateAssetRef}
			} else {
				payload = bson.M{"estate_asset": metadata.EstateAssetRef}
			}
			err := dbCollection.First(payload, existing)
			found := true
			if err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					return err
				}
				found = false
			}
			if found {
				metadata.ID = existing.ID
				err = dbCollection.Update(metadata)
				if err != nil {
					return err
				}
			} else {
				err = dbCollection.Create(metadata)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
