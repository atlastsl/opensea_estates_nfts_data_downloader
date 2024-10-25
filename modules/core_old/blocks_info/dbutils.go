package blocks_info

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"reflect"
	"time"
)

func getDistinctBlockNumbersFromDatabase(collection collections.Collection, dbInstance *mongo.Database) ([]uint64, error) {
	dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	matchStage := bson.D{
		{"$match", bson.D{{"collection", string(collection)}}},
	}
	groupStage := bson.D{
		{"$group", bson.D{
			{"_id", "$block_number"},
		}},
	}
	sortStage := bson.D{
		{"$sort", bson.D{{"_id", 1}}},
	}
	asArrayStage := bson.D{
		{"$group", bson.D{
			{"_id", nil},
			{"blockNumbers", bson.D{{"$push", "$_id"}}},
		}},
	}
	cursor, err := dbCollection.Aggregate(context.Background(), mongo.Pipeline{matchStage, groupStage, sortStage, asArrayStage})
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
	blockNumbers := make([]uint64, 0)
	for _, item := range tmp.(primitive.A) {
		if reflect.TypeOf(item).Kind() == reflect.Int {
			blockNumbers = append(blockNumbers, uint64(item.(int64)))
		} else if reflect.TypeOf(item).Kind() == reflect.Int32 {
			blockNumbers = append(blockNumbers, uint64(item.(int32)))
		} else if reflect.TypeOf(item).Kind() == reflect.Int64 {
			blockNumbers = append(blockNumbers, uint64(item.(int64)))
		}
	}
	return blockNumbers, nil
}

func saveBlockTimestampInDatabase(blockInfos []*helpers.EthBlockInfo, collection collections.Collection, dbInstance *mongo.Database) error {
	if blockInfos != nil && len(blockInfos) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})

		operations := make([]mongo.WriteModel, len(blockInfos))
		for i, info := range blockInfos {
			var filterPayload = bson.M{"collection": collection, "block_number": info.BlockNumber}
			var updatePayload = bson.M{"$set": bson.M{"block_timestamp": info.BlockTimestamp, "updated_at": time.Now()}}
			operations[i] = mongo.NewUpdateManyModel().SetFilter(filterPayload).SetUpdate(updatePayload).SetUpsert(false)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
