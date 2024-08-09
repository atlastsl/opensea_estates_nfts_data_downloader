package eth_events

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getLatestFetchedBlockNumber(collection collections.Collection, chain string, dbInstance *mongo.Database) (map[string]uint64, error) {
	dbCollection := database.CollectionInstance(dbInstance, &BlockNumber{})
	cursor, err := dbCollection.Find(context.Background(), bson.M{"collection": string(collection), "chain": chain})
	defer cursor.Close(context.Background())
	blockNumbers := make([]BlockNumber, 0)
	err = cursor.All(context.Background(), &blockNumbers)
	if err != nil {
		return nil, err
	}
	result := make(map[string]uint64)
	for _, number := range blockNumbers {
		result[number.Topic] = number.LatestFetched
	}
	return result, nil
}

func saveLatestFetchedBlockNumber(collection collections.Collection, chain string, topic string, newValue uint64, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &BlockNumber{})
	forOptions := &options.FindOneAndUpdateOptions{}
	payload := bson.M{"$set": bson.M{"latest_fetched": newValue, "collection": string(collection), "chain": chain, "topic": topic}}
	result := dbCollection.FindOneAndUpdate(context.Background(), bson.M{"collection": string(collection), "topic": topic}, payload, forOptions.SetUpsert(true))
	return result.Err()
}

func saveEventsInDatabase(events []*EthEvent, dbInstance *mongo.Database) error {
	if events != nil && len(events) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EthEvent{})

		operations := make([]mongo.WriteModel, len(events))
		for i, event := range events {
			var filterPayload = bson.M{"collection": event.Collection, "address": event.Address, "event_id": event.EventId, "transaction_hash": event.TransactionHash}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(event).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
