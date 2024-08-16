package movements

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"os"
	"strings"
)

type TxHash struct {
	hash      string
	timestamp int64
}

func getAllEventsTransactionsHashes(collection collections.Collection, dbInstance *mongo.Database) ([]string, map[string]*TxHash, error) {
	assetEvtDbCol := database.CollectionInstance(dbInstance, &ops_events.EstateEvent{})
	matchStage := bson.D{
		{"$match", bson.D{{"collection", string(collection)}}},
	}
	hashStage := bson.D{
		{"$addFields", bson.D{{"transaction_hash", bson.D{{"$ifNull", bson.A{"$transaction", "$fixed_transaction"}}}}}},
	}
	fSortStage := bson.D{
		{"$sort", bson.D{{"evt_timestamp", 1}}},
	}
	groupStage := bson.D{
		{"$group", bson.D{
			{"_id", "$transaction_hash"},
			{"nb", bson.D{{"$sum", 1}}},
			{"timestamp", bson.D{{"$max", "$evt_timestamp"}}},
		}},
	}
	sSortStage := bson.D{
		{"$sort", bson.D{{"timestamp", 1}}},
	}
	skipStage := bson.D{
		{"$skip", 0},
	}
	_ = bson.D{
		{"$limit", 20},
	}
	cursor, err := assetEvtDbCol.Aggregate(context.Background(), mongo.Pipeline{matchStage, hashStage, fSortStage, groupStage, sSortStage, skipStage})
	//cursor, err := assetEvtDbCol.Aggregate(context.Background(), mongo.Pipeline{matchStage, fSortStage, groupStage, sSortStage})
	if err != nil {
		return nil, nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]bson.M, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, nil, err
	}
	transactions := make(map[string]*TxHash)
	txHashes := make([]string, len(results))
	for i, result := range results {
		hash := result["_id"].(string)
		timestamp := result["timestamp"].(int64)
		txHashes[i] = hash
		transactions[hash] = &TxHash{hash: hash, timestamp: timestamp}
	}
	return txHashes, transactions, nil
}

func getAllEstateAssets(collection collections.Collection, dbInstance *mongo.Database) ([]*assets.EstateAsset, error) {
	dbCollection := database.CollectionInstance(dbInstance, &assets.EstateAsset{})
	cursor, err := dbCollection.Find(context.Background(), bson.M{"collection": string(collection)})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*assets.EstateAsset, 0)
	err = cursor.All(context.Background(), &results)
	return results, err
}

func getCurrencyPrices(collection collections.Collection, dbInstance *mongo.Database) (map[string][]*CurrencyPrice, error) {
	dbCollection := database.CollectionInstance(dbInstance, &CurrencyPrice{})
	currencies := make([]string, 0)
	if collection == collections.CollectionDcl {
		currencies = strings.Split(os.Getenv("CURRENCIES_DCL"), ",")
	}
	prices := make(map[string][]*CurrencyPrice)
	for _, currency := range currencies {
		cursor, err := dbCollection.Find(context.Background(), bson.M{"currency": currency})
		if err != nil {
			return nil, nil
		}
		currencyPrices := make([]*CurrencyPrice, 0)
		err = cursor.All(context.Background(), &currencyPrices)
		if err != nil {
			return nil, nil
		}
		_ = cursor.Close(context.Background())
		prices[currency] = currencyPrices
	}

	return prices, nil
}

func getEthEventsLogsByTransactionHash(collection collections.Collection, transactionHash string, dbInstance *mongo.Database) ([]*eth_events.EthEvent, error) {
	ethEvtLogsDbCol := database.CollectionInstance(dbInstance, &eth_events.EthEvent{})
	cursor, err := ethEvtLogsDbCol.Find(context.Background(), bson.M{"collection": string(collection), "transaction_hash": transactionHash})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	events := make([]*eth_events.EthEvent, 0)
	err = cursor.All(context.Background(), &events)
	return events, err
}

func getEstateEventsLogsByTransactionHash(collection collections.Collection, transactionHash string, dbInstance *mongo.Database) ([]*ops_events.EstateEvent, error) {
	assetEvtDbCol := database.CollectionInstance(dbInstance, &ops_events.EstateEvent{})
	cursor, err := assetEvtDbCol.Find(context.Background(), bson.M{"collection": string(collection), "$or": bson.A{bson.M{"transaction": transactionHash}, bson.M{"fixed_transaction": transactionHash}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	events := make([]*ops_events.EstateEvent, 0)
	err = cursor.All(context.Background(), &events)
	return events, err
}

func getMetadataByEstateAsset(asset *assets.EstateAsset, metadataName string, dbInstance *mongo.Database) (*assets.EstateAssetMetadata, error) {
	metadataItem := &assets.EstateAssetMetadata{}
	dbCollection := database.CollectionInstance(dbInstance, metadataItem)
	result := dbCollection.FindOne(context.Background(), bson.M{"estate_asset": asset.ID, "name": metadataName}, &options.FindOneOptions{Sort: bson.M{"update_date": -1}})
	if result.Err() != nil {
		if !errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return nil, result.Err()
		} else {
			metadataItem = nil
		}
	} else {
		err := result.Decode(metadataItem)
		if err != nil {
			return nil, err
		}
	}
	return metadataItem, nil
}

func getCoordinatesOfLandsByIdentifiers(collection, contract string, identifiers []string, dbInstance *mongo.Database) ([]string, error) {
	estateAsset := &assets.EstateAsset{}
	dbCollection := database.CollectionInstance(dbInstance, estateAsset)
	filterPayload := bson.D{{"collection", collection}, {"contract", contract}, {"identifier", bson.D{{"$in", identifiers}}}}
	cursor, err := dbCollection.Find(context.Background(), filterPayload)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]assets.EstateAsset, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	coords := helpers.ArrayMap(results, func(t assets.EstateAsset) (bool, string) {
		return true, fmt.Sprintf("%d,%d", t.X, t.Y)
	}, true, "")
	return coords, nil
}

func getDistancesByEstateAssetLands(collection, contract string, coords []string, dbInstance *mongo.Database) ([]*tiles_distances.MapTileMacroDistance, error) {
	dbCollection := database.CollectionInstance(dbInstance, &tiles_distances.MapTileMacroDistance{})
	tilesSlugs := helpers.ArrayMap(coords, func(t string) (bool, string) {
		return true, fmt.Sprintf("%s|%s|%s", collection, contract, t)
	}, true, "")
	cursor, err := dbCollection.Find(context.Background(), bson.M{"tile_slug": bson.M{"$in": helpers.BSONStringA(tilesSlugs)}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	distances := make([]*tiles_distances.MapTileMacroDistance, 0)
	err = cursor.All(context.Background(), &distances)
	return distances, err
}

func saveMetadataInDatabase(assetMetadata []*assets.EstateAssetMetadata, dbInstance *mongo.Database) error {
	if assetMetadata != nil && len(assetMetadata) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &assets.EstateAssetMetadata{})
		operations := make([]mongo.WriteModel, len(assetMetadata))
		for i, metadata := range assetMetadata {
			var filterPayload bson.M
			if !metadata.MacroRef.IsZero() {
				filterPayload = bson.M{"macro": metadata.MacroRef, "estate_asset": metadata.EstateAssetRef, "update_date": metadata.UpdateDate}
			} else {
				filterPayload = bson.M{"estate_asset": metadata.EstateAssetRef, "update_date": metadata.UpdateDate}
			}
			if metadata.MetadataType == assets.MetadataTypeDistance {
				filterPayload["metadata_type"] = metadata.MetadataType
				filterPayload["macro_type"] = metadata.MacroType
			} else {
				filterPayload["name"] = metadata.Name
			}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(metadata).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}

func saveMovementsInDatabase(movements []*AssetMovement, dbInstance *mongo.Database) error {
	if movements != nil && len(movements) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &AssetMovement{})

		operations := make([]mongo.WriteModel, len(movements))
		for i, movement := range movements {
			var filterPayload = bson.M{"asset_ref": movement.AssetRef, "movement": movement.Movement, "tx_hash": movement.TxHash}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(movement).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err

	}
	return nil
}
