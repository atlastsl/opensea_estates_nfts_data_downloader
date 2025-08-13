package operations

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"reflect"
)

func getAssetFromDatabase(metaverse, contract, assetId string, dbInstance *mongo.Database) (*metaverses.MetaverseAsset, error) {
	asset := &metaverses.MetaverseAsset{}
	dbCollection := database.CollectionInstance(dbInstance, asset)
	payload := bson.M{"metaverse": metaverse, "contract": contract, "asset_id": assetId}
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

func getDistinctBlocksNumbers(metaverse string, dbInstance *mongo.Database) ([]*BlockNumberInput, error) {
	dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	matchStage := bson.D{
		{"$match", bson.D{{"metaverse", metaverse}, {"block_number", bson.D{{"$gte", 6675885}}}}},
	}
	groupStage := bson.D{
		{"$group", bson.D{
			{"_id", bson.D{
				{"blockchain", "$blockchain"},
				{"block_number", "$block_number"},
			}},
			{"timestamp", bson.D{
				{"$max", "$block_timestamp"},
			}},
		}},
	}
	sortStage := bson.D{
		{"$sort", bson.D{{"timestamp", 1}}},
	}
	skipStage := bson.D{
		{"$skip", 0},
	}
	limitStage := bson.D{
		{"$limit", 1000000},
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
	tmpStr, err := json.Marshal(tmp)
	if err != nil {
		return nil, err
	}
	blockNumbers := make([]*BlockNumberInput, 0)
	err = json.Unmarshal(tmpStr, &blockNumbers)
	if err != nil {
		return nil, err
	}
	return blockNumbers, nil
}

func getTransactionInfoByBlockNumber(blockchain string, blockNumber int, dbInstance *mongo.Database) ([]*TransactionFull, error) {
	txInfoDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionInfo{})
	txLogsDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionLog{})

	cursor, err := txInfoDbTable.Find(context.Background(), bson.M{"blockchain": blockchain, "block_number": blockNumber})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	txInfos := make([]*transactions_infos.TransactionInfo, 0)
	err = cursor.All(context.Background(), &txInfos)
	if err != nil {
		return nil, err
	}

	cursor, err = txLogsDbTable.Find(context.Background(), bson.M{"blockchain": blockchain, "block_number": blockNumber})
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

func getTransactionInfoByBlockNumbers(blockNumbers []*BlockNumberInput, dbInstance *mongo.Database) (map[string][]*TransactionFull, error) {
	txInfoDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionInfo{})
	txLogsDbTable := database.CollectionInstance(dbInstance, &transactions_infos.TransactionLog{})

	blockNumbersPerChain := make(map[string][]int)
	for _, item := range blockNumbers {
		_, ok := blockNumbersPerChain[item.Blockchain]
		if !ok {
			blockNumbersPerChain[item.Blockchain] = make([]int, 0)
		}
		blockNumbersPerChain[item.Blockchain] = append(blockNumbersPerChain[item.Blockchain], item.BlockNumber)
	}

	txInfos := make([]*transactions_infos.TransactionInfo, 0)
	txLogs := make([]*transactions_infos.TransactionLog, 0)

	for blockchain, _blockNumbers := range blockNumbersPerChain {
		cursor, err := txInfoDbTable.Find(context.Background(), bson.M{"blockchain": blockchain, "block_number": bson.M{"$in": helpers.BSONIntA(_blockNumbers)}})
		if err != nil {
			return nil, err
		}
		bTxInfos := make([]*transactions_infos.TransactionInfo, 0)
		err = cursor.All(context.Background(), &bTxInfos)
		if err != nil {
			return nil, err
		}
		_ = cursor.Close(context.Background())
		txInfos = append(txInfos, bTxInfos...)

		cursor, err = txLogsDbTable.Find(context.Background(), bson.M{"blockchain": blockchain, "block_number": bson.M{"$in": helpers.BSONIntA(_blockNumbers)}})
		if err != nil {
			return nil, err
		}
		bTxLogs := make([]*transactions_infos.TransactionLog, 0)
		err = cursor.All(context.Background(), &bTxLogs)
		if err != nil {
			return nil, err
		}
		_ = cursor.Close(context.Background())
		txLogs = append(txLogs, bTxLogs...)
	}

	result := make(map[string][]*TransactionFull)
	for _, txInfo := range txInfos {
		tTxLogs := helpers.ArrayFilter(txLogs, func(log *transactions_infos.TransactionLog) bool {
			return log.TransactionHash == txInfo.TransactionHash
		})
		blockNumber := txInfo.BlockNumber
		task := fmt.Sprintf("%s_%d", txInfo.Blockchain, int64(blockNumber))
		if len(tTxLogs) > 0 {
			updateTransactionsList(task, &TransactionFull{Transaction: txInfo, Logs: tTxLogs}, &result)
		}
	}

	return result, nil
}

func getCoordinatesOfLandsByIdentifiers(metaverse, contract string, identifiers []string, dbInstance *mongo.Database) ([]string, error) {
	dbCollection := database.CollectionInstance(dbInstance, &metaverses.MetaverseAsset{})
	filterPayload := bson.D{{"metaverse", metaverse}, {"contract", contract}, {"asset_id", bson.D{{"$in", helpers.BSONStringA(identifiers)}}}}
	cursor, err := dbCollection.Find(context.Background(), filterPayload)
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*metaverses.MetaverseAsset, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	coords := helpers.ArrayMap(results, func(t *metaverses.MetaverseAsset) (bool, string) {
		return true, t.Location
	}, true, "")
	return coords, nil
}

func saveOperationsInDatabase(operations []*Operation, dbInstance *mongo.Database) error {
	if operations != nil && len(operations) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &Operation{})

		bdOperations := make([]mongo.WriteModel, len(operations))
		for i, operation := range operations {
			var filterPayload = bson.M{"metaverse": operation.Metaverse, "asset_contract": operation.AssetContract, "asset_id": operation.AssetId, "operation_type": operation.OperationType, "transaction_hash": operation.TransactionHash}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(operation).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func getCurrencies(dbInstance *mongo.Database) (map[string]*metaverses.Currency, error) {
	dbCollection := database.CollectionInstance(dbInstance, &metaverses.Currency{})
	cursor, err := dbCollection.Find(context.Background(), bson.D{})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make(map[string]*metaverses.Currency, 0)
	for cursor.Next(context.Background()) {
		currency := &metaverses.Currency{}
		err = cursor.Decode(currency)
		if err != nil {
			return nil, err
		}
		results[currency.Contract] = currency
	}
	return results, nil
}

func getCurrencyPrices(dbInstance *mongo.Database) (map[string][]*metaverses.CurrencyPrice, error) {
	curCollection := database.CollectionInstance(dbInstance, &metaverses.Currency{})
	rawCurrencies, err := curCollection.Distinct(context.Background(), "symbols", bson.M{})
	if err != nil {
		return nil, err
	}
	currencies := make([]string, 0)
	for _, currency := range rawCurrencies {
		currencies = append(currencies, currency.(string))
	}
	dbCollection := database.CollectionInstance(dbInstance, &metaverses.CurrencyPrice{})

	prices := make(map[string][]*metaverses.CurrencyPrice)
	for _, currency := range currencies {
		cursor, err := dbCollection.Find(context.Background(), bson.M{"currency": currency}, &options.FindOptions{Sort: bson.M{"start": 1}})
		if err != nil {
			return nil, nil
		}
		currencyPrices := make([]*metaverses.CurrencyPrice, 0)
		err = cursor.All(context.Background(), &currencyPrices)
		if err != nil {
			return nil, nil
		}
		_ = cursor.Close(context.Background())
		prices[currency] = currencyPrices
	}

	return prices, nil
}

func getUpdatableAttrOfAsset(asset *metaverses.MetaverseAsset, attrName string, dbInstance *mongo.Database) (*AssetChange, error) {
	operation := &Operation{}
	var currentValue *AssetChange
	dbCollection := database.CollectionInstance(dbInstance, operation)
	var filterPayload = bson.M{"metaverse": asset.Metaverse, "asset_contract": asset.Contract, "asset_id": asset.AssetId, "asset_changes": bson.M{"attr_name": attrName}}
	result := dbCollection.FindOne(context.Background(), filterPayload, &options.FindOneOptions{Sort: bson.M{"date": -1}})
	if result.Err() != nil {
		if !errors.Is(result.Err(), mongo.ErrNoDocuments) {
			return nil, result.Err()
		}
	} else {
		err := result.Decode(operation)
		if err != nil {
			return nil, err
		}
	}
	for _, change := range operation.AssetChanges {
		if change.AttrName == attrName {
			currentValue = &change
		}
	}
	return currentValue, nil
}

//func fetchTileMacroDistances(collection collections.Collection, contract string, dbInstance *mongo.Database) ([]*tiles_distances.MapTileMacroDistance, error) {
//	tmDistancesCol := database.CollectionInstance(dbInstance, &tiles_distances.MapTileMacroDistance{})
//	regexPattern := fmt.Sprintf("%s|%s|", string(collection), contract)
//	cursor, err := tmDistancesCol.Find(context.Background(), bson.M{"tile_slug": bson.M{"$regex": primitive.Regex{Pattern: regexPattern, Options: "i"}}})
//	if err != nil {
//		return nil, err
//	}
//	var distances []*tiles_distances.MapTileMacroDistance
//	err = cursor.All(context.Background(), &distances)
//	if err != nil {
//		return nil, err
//	}
//	err = cursor.Close(context.Background())
//	if err != nil {
//		return nil, err
//	}
//	return distances, nil
//}
