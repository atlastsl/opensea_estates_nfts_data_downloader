package movements

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"math/big"
	"time"
)

func getAssetEventsFromDatabase(collection collections.Collection, skip, limit int64, dbInstance *mongo.Database) ([]string, error) {
	assetEvtDbCol := database.CollectionInstance(dbInstance, &ops_events.EstateEvent{})
	matchStage := bson.D{
		{"$match", bson.D{{"collection", string(collection)}}},
	}
	groupStage := bson.D{
		{"$group", bson.D{{"_id", "$transaction"}, {"nb", bson.D{{"sum", 1}}}}},
	}
	sortStage := bson.D{
		{"$sort", bson.D{{"evt_timestamp", 1}}},
	}
	skipStage := bson.D{
		{"$skip", skip},
	}
	limitStage := bson.D{
		{"$limit", limit},
	}
	cursor, err := assetEvtDbCol.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, sortStage, skipStage, limitStage})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]bson.M, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	transactions := helpers.ArrayMap(results, func(t bson.M) (bool, string) {
		return true, t["_id"].(string)
	}, true, "")
	return transactions, nil
}

func getEthLogsByTransactionHash(collection collections.Collection, transactionHash string, dbInstance *mongo.Database) ([]eth_events.EthEvent, error) {
	ethEvtLogsDbCol := database.CollectionInstance(dbInstance, &eth_events.EthEvent{})
	cursor, err := ethEvtLogsDbCol.Find(context.Background(), bson.M{"collection": string(collection), "transaction_hash": transactionHash})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	events := make([]eth_events.EthEvent, 0)
	err = cursor.All(context.Background(), &events)
	return events, err
}

func getEstateEventsByTransactionHash(collection collections.Collection, transactionHash string, dbInstance *mongo.Database) ([]ops_events.EstateEvent, error) {
	assetEvtDbCol := database.CollectionInstance(dbInstance, &ops_events.EstateEvent{})
	cursor, err := assetEvtDbCol.Find(context.Background(), bson.M{"collection": string(collection), "transaction_hash": transactionHash})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	events := make([]ops_events.EstateEvent, 0)
	err = cursor.All(context.Background(), &events)
	return events, err
}

func getEstateAsset(collection, contract, identifier string, dbInstance *mongo.Database) (*assets.EstateAsset, *assets.EstateAssetMetadata, error) {
	estateAsset := &assets.EstateAsset{}
	estateAssetsCol := database.CollectionInstance(dbInstance, estateAsset)
	err := estateAssetsCol.First(bson.M{"collection": collection, "contract": contract, "identifier": identifier}, estateAsset)
	if err != nil {
		return nil, nil, err
	}
	estateAssetLandsMtd := &assets.EstateAssetMetadata{}
	estateAssetMetadataCol := database.CollectionInstance(dbInstance, estateAssetLandsMtd)
	err = estateAssetMetadataCol.First(bson.M{"estate_asset": estateAsset.ID}, estateAssetLandsMtd)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, nil, err
		} else {
			estateAssetLandsMtd = nil
		}
	}
	return estateAsset, estateAssetLandsMtd, nil
}

func getLandsCoords(collection, contract string, identifiers []string, dbInstance *mongo.Database) ([]bson.M, error) {
	estateAsset := &assets.EstateAsset{}
	estateAssetsCol := database.CollectionInstance(dbInstance, estateAsset)
	matchStage := bson.D{
		{"$match", bson.D{{"collection", collection}, {"contract", contract}, {"identifier", bson.D{{"$in", identifiers}}}}},
	}
	projectStage := bson.D{
		{"$project", bson.D{{"collection", 1}, {"contract", 1}, {"identifier", 1}, {"coords", 1}}},
	}
	cursor, err := estateAssetsCol.Aggregate(context.Background(), mongo.Pipeline{matchStage, projectStage})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]bson.M, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func getMacros(collection, contract string, dbInstance *mongo.Database) ([]tiles.MapMacro, error) {
	macroCol := database.CollectionInstance(dbInstance, &tiles.MapMacro{})
	cursor, err := macroCol.Find(context.Background(), bson.M{"collection": collection, "contract": contract})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]tiles.MapMacro, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func getDistanceToMacro(macro *tiles.MapMacro, tiles []string, dbInstance *mongo.Database) (*tiles_distances.MapTileMacroDistance, error) {
	distancesCol := database.CollectionInstance(dbInstance, &tiles_distances.MapTileMacroDistance{})
	payload := bson.M{}
	if len(tiles) > 0 {
		payload = bson.M{"macro_ref": macro.ID, "tile_slug": bson.M{"$in": tiles}}
	} else {
		payload = bson.M{"macro_ref": macro.ID}
	}
	res := distancesCol.FindOne(context.Background(), payload, &options.FindOneOptions{Sort: bson.M{"man_distance": 1}})
	if res.Err() != nil && !errors.Is(res.Err(), mongo.ErrNoDocuments) {
		return nil, res.Err()
	}
	result := &tiles_distances.MapTileMacroDistance{}
	err := res.Decode(result)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func saveMetadataInDatabase(assetMetadata []*assets.EstateAssetMetadata, dbInstance *mongo.Database) error {
	if assetMetadata != nil && len(assetMetadata) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &assets.EstateAssetMetadata{})
		for _, metadata := range assetMetadata {
			existing := &assets.EstateAssetMetadata{}
			var payload bson.M
			if !metadata.MacroRef.IsZero() {
				payload = bson.M{"macro": metadata.MacroRef, "estate_asset": metadata.EstateAssetRef, "update_date": metadata.UpdateDate}
			} else {
				payload = bson.M{"estate_asset": metadata.EstateAssetRef, "update_date": metadata.UpdateDate}
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

func getCurrencyPrice(targetTime time.Time, currency string, dbInstance *mongo.Database) (float64, error) {
	ccyPriceCol := database.CollectionInstance(dbInstance, &CurrencyPrice{})
	start := targetTime.Format(time.DateOnly)
	end := targetTime.Add(24 * time.Hour).Format(time.DateOnly)
	priceInstance := &CurrencyPrice{}
	err := ccyPriceCol.First(bson.M{"currency": currency, "start": start, "end": end}, priceInstance)
	if err != nil {
		return 0, nil
	}
	openP := new(big.Float).SetFloat64(priceInstance.Open)
	closeP := new(big.Float).SetFloat64(priceInstance.Close)
	highP := new(big.Float).SetFloat64(priceInstance.High)
	lowP := new(big.Float).SetFloat64(priceInstance.Low)
	price := new(big.Float).Add(openP, closeP)
	price = price.Add(price, highP)
	price = price.Add(price, lowP)
	price = price.Quo(price, new(big.Float).SetFloat64(4.0))
	fPrice, _ := price.Float64()
	return fPrice, nil
}

func saveMovementsInDatabase(movements []*AssetMovement, dbInstance *mongo.Database) error {
	if movements != nil && len(movements) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &assets.EstateAssetMetadata{})
		for _, movement := range movements {
			existing := &AssetMovement{}
			err := dbCollection.First(bson.M{"asset_ref": movement.AssetRef, "movement": movement.Movement, "tx_hash": movement.TxHash}, existing)
			found := true
			if err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					return err
				}
				found = false
			}
			if found {
				movement.ID = existing.ID
				err = dbCollection.Update(movement)
				if err != nil {
					return err
				}
			} else {
				err = dbCollection.Create(movement)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}
