package operations

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
	"strconv"
)

func getAssetFromDatabase(collection, contract, assetId string, dbInstance *mongo.Database) (*Asset, error) {
	asset := &Asset{}
	dbCollection := database.CollectionInstance(dbInstance, asset)
	payload := bson.M{"collection": collection, "contract": contract, "asset_id": assetId}
	err := dbCollection.FirstWithCtx(context.Background(), payload, asset)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			asset = nil
		}
	}
	return asset, nil
}

func saveAssetInDatabase(asset *Asset, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, asset)
	payload := bson.M{"collection": asset.Collection, "contract": asset.Contract, "asset_id": asset.AssetId}
	opts := &options.ReplaceOptions{}
	_, err := dbCollection.ReplaceOne(context.Background(), payload, asset, opts.SetUpsert(true))
	if err != nil {
		return err
	}
	return nil
}

func saveAssetMetadataInDatabase(assetMetadataList []*AssetMetadata, dbInstance *mongo.Database) error {
	if assetMetadataList != nil && len(assetMetadataList) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &AssetMetadata{})
		operations := make([]mongo.WriteModel, len(assetMetadataList))
		for i, metadata := range assetMetadataList {
			payload := bson.M{"collection": metadata.Collection, "asset_contract": metadata.AssetContract, "asset_id": metadata.AssetId}
			if !metadata.MacroRef.IsZero() {
				payload["macro"] = metadata.MacroRef
			} else {
				payload["name"] = metadata.Name
			}
			if !metadata.Date.IsZero() {
				payload["date"] = metadata.Date
			}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(payload).SetReplacement(metadata).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}

func getNftCollectionInfo(collection collections.Collection, dbInstance *mongo.Database) (*collections.CollectionInfo, error) {
	cltInfo := &collections.CollectionInfo{}
	dbCollection := database.CollectionInstance(dbInstance, cltInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": string(collection)}, cltInfo)
	if err != nil {
		return nil, err
	}
	return cltInfo, nil
}

func getDistinctBlocksNumbers(collection string, dbInstance *mongo.Database) ([]int, error) {
	dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	matchStage := bson.D{
		{"$match", bson.D{{"collection", collection}}},
	}
	groupStage := bson.D{
		{"$group", bson.D{
			{"_id", "$block_number"},
		}},
	}
	sortStage := bson.D{
		{"$sort", bson.D{{"_id", 1}}},
	}
	skipStage := bson.D{
		{"$skip", 0},
	}
	limitStage := bson.D{
		{"$limit", 70000},
	}
	asArrayStage := bson.D{
		{"$group", bson.D{
			{"_id", nil},
			{"blockNumbers", bson.D{{"$push", "$_id"}}},
		}},
	}
	cursor, err := dbCollection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, sortStage, skipStage, limitStage, asArrayStage})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]bson.M, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	if len(results) == 0 {
		return nil, errors.New("no results found")
	}
	tmp := results[0]["blockNumbers"]
	if reflect.TypeOf(tmp).Kind() != reflect.Slice {
		return nil, errors.New("block numbers is not a slice")
	}
	blockNumbers := make([]int, 0)
	for _, item := range tmp.(primitive.A) {
		if reflect.TypeOf(item).Kind() == reflect.Int {
			blockNumbers = append(blockNumbers, int(item.(int64)))
		} else if reflect.TypeOf(item).Kind() == reflect.Int32 {
			blockNumbers = append(blockNumbers, int(item.(int32)))
		} else if reflect.TypeOf(item).Kind() == reflect.Int64 {
			blockNumbers = append(blockNumbers, int(item.(int64)))
		}
	}
	return blockNumbers, nil
}

func getTransactionInfoByBlockNumber(blockNumber int, dbInstance *mongo.Database) ([]*TransactionFull, error) {
	txInfoDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionInfo{})
	txLogsDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionLog{})

	cursor, err := txInfoDbTable.Find(context.Background(), bson.M{"block_number": blockNumber})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	txInfos := make([]*transactions_infos.TransactionInfo, 0)
	err = cursor.All(context.Background(), &txInfos)
	if err != nil {
		return nil, err
	}

	cursor, err = txLogsDbTable.Find(context.Background(), bson.M{"block_number": blockNumber})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	txLogs := make([]*transactions_infos.TransactionLog, 0)
	err = cursor.All(context.Background(), &txLogs)
	if err != nil {
		return nil, err
	}

	transactions := make([]*TransactionFull, 0)
	for _, txInfo := range txInfos {
		tTxLogs := helpers.ArrayFilter(txLogs, func(log *transactions_infos.TransactionLog) bool {
			return log.TransactionHash == txInfo.TransactionHash
		})
		if len(tTxLogs) > 0 {
			transactions = append(transactions, &TransactionFull{Transaction: txInfo, Logs: tTxLogs})
		}
	}
	return transactions, nil
}

func updateTransactionsList(key string, item *TransactionFull, table *map[string][]*TransactionFull) {
	_, found := (*table)[key]
	if !found {
		(*table)[key] = make([]*TransactionFull, 0)
	}
	(*table)[key] = append((*table)[key], item)
}

func getTransactionInfoByBlockNumbers(blockNumbers []int, dbInstance *mongo.Database) (map[string][]*TransactionFull, error) {
	txInfoDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionInfo{})
	txLogsDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionLog{})

	cursor, err := txInfoDbTable.Find(context.Background(), bson.M{"block_number": bson.M{"$in": helpers.BSONIntA(blockNumbers)}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	txInfos := make([]*transactions_infos.TransactionInfo, 0)
	err = cursor.All(context.Background(), &txInfos)
	if err != nil {
		return nil, err
	}

	cursor, err = txLogsDbTable.Find(context.Background(), bson.M{"block_number": bson.M{"$in": helpers.BSONIntA(blockNumbers)}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	txLogs := make([]*transactions_infos.TransactionLog, 0)
	err = cursor.All(context.Background(), &txLogs)
	if err != nil {
		return nil, err
	}

	result := make(map[string][]*TransactionFull)
	for _, txInfo := range txInfos {
		tTxLogs := helpers.ArrayFilter(txLogs, func(log *transactions_infos.TransactionLog) bool {
			return log.TransactionHash == txInfo.TransactionHash
		})
		blockNumber := txInfo.BlockNumber
		task := strconv.FormatInt(int64(blockNumber), 10)
		if len(tTxLogs) > 0 {
			updateTransactionsList(task, &TransactionFull{Transaction: txInfo, Logs: tTxLogs}, &result)
		}
	}

	return result, nil
}

func getMetadataByEstateAsset(asset *Asset, metadataName string, dbInstance *mongo.Database) (*AssetMetadata, error) {
	metadataItem := &AssetMetadata{}
	dbCollection := database.CollectionInstance(dbInstance, metadataItem)
	payload := bson.M{"collection": asset.Collection, "asset_contract": asset.Contract, "asset_id": asset.AssetId, "name": metadataName}
	result := dbCollection.FindOne(context.Background(), payload, &options.FindOneOptions{Sort: bson.M{"date": -1}})
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
	dbCollection := database.CollectionInstance(dbInstance, &Asset{})
	filterPayload := bson.D{{"collection", collection}, {"contract", contract}, {"asset_id", bson.D{{"$in", helpers.BSONStringA(identifiers)}}}}
	cursor, err := dbCollection.Find(context.Background(), filterPayload)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*Asset, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	coords := helpers.ArrayMap(results, func(t *Asset) (bool, string) {
		return true, fmt.Sprintf("%d,%d", t.X, t.Y)
	}, true, "")
	return coords, nil
}

func saveOperationsInDatabase(operations []*Operation, dbInstance *mongo.Database) error {
	if operations != nil && len(operations) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &Operation{})

		bdOperations := make([]mongo.WriteModel, len(operations))
		for i, operation := range operations {
			var filterPayload = bson.M{"collection": operation.Collection, "asset_contract": operation.AssetContract, "asset_id": operation.AssetId, "operation_type": operation.OperationType, "transaction_hash": operation.TransactionHash}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(operation).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func getCurrencies(dbInstance *mongo.Database) (map[string]*collections.Currency, error) {
	dbCollection := database.CollectionInstance(dbInstance, &collections.Currency{})
	cursor, err := dbCollection.Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make(map[string]*collections.Currency, 0)
	for cursor.Next(context.Background()) {
		currency := &collections.Currency{}
		err = cursor.Decode(currency)
		if err != nil {
			return nil, err
		}
		results[currency.Contract] = currency
	}
	return results, nil
}

func getCurrencyPrices(dbInstance *mongo.Database) (map[string][]*collections.CurrencyPrice, error) {
	curCollection := database.CollectionInstance(dbInstance, &collections.Currency{})
	rawCurrencies, err := curCollection.Distinct(context.Background(), "symbols", bson.M{})
	if err != nil {
		return nil, err
	}
	currencies := make([]string, 0)
	for _, currency := range rawCurrencies {
		currencies = append(currencies, currency.(string))
	}
	dbCollection := database.CollectionInstance(dbInstance, &collections.CurrencyPrice{})

	prices := make(map[string][]*collections.CurrencyPrice)
	for _, currency := range currencies {
		cursor, err := dbCollection.Find(context.Background(), bson.M{"currency": currency}, &options.FindOptions{Sort: bson.M{"start": 1}})
		if err != nil {
			return nil, nil
		}
		currencyPrices := make([]*collections.CurrencyPrice, 0)
		err = cursor.All(context.Background(), &currencyPrices)
		if err != nil {
			return nil, nil
		}
		_ = cursor.Close(context.Background())
		prices[currency] = currencyPrices
	}

	return prices, nil
}

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
