package eth_events

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func getLatestFetchedBlockNumber(collection collections.Collection, chain string, dbInstance *mongo.Database) (uint64, error) {
	dbCollection := database.CollectionInstance(dbInstance, &BlockNumber{})
	bnRecord := &BlockNumber{}
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"collection": string(collection), "chain": chain}, bnRecord)
	found := true
	defaultValue := uint64(0)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return defaultValue, err
		}
		found = false
	}
	if !found {
		return defaultValue, nil
	} else {
		return bnRecord.LatestFetched, nil
	}
}

func saveLatestFetchedBlockNumber(collection collections.Collection, chain string, newValue uint64, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &BlockNumber{})
	forOptions := &options.FindOneAndUpdateOptions{}
	payload := bson.M{"$set": bson.M{"latest_fetched": newValue, "collection": string(collection), "chain": chain}}
	result := dbCollection.FindOneAndUpdate(context.Background(), bson.M{"collection": string(collection)}, payload, forOptions.SetUpsert(true))
	return result.Err()
}

func saveLatestTrueBlockNumber(collection collections.Collection, chain string, newValue uint64, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &BlockNumber{})
	forOptions := &options.FindOneAndUpdateOptions{}
	payload := bson.M{"$set": bson.M{"latest_true": newValue, "collection": string(collection), "chain": chain}}
	result := dbCollection.FindOneAndUpdate(context.Background(), bson.M{"collection": string(collection)}, payload, forOptions.SetUpsert(true))
	return result.Err()
}

func saveEventsInDatabase(events []*EthEvent, dbInstance *mongo.Database) error {
	if events != nil && len(events) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EthEvent{})

		operations := make([]mongo.WriteModel, len(events))
		for i, event := range events {
			var filterPayload = bson.M{"collection": event.Collection, "address": event.Address, "event_id": event.EventId}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(event).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
