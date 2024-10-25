package ops_events

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func saveOpsEventInDatabase(events []*EstateEvent, dbInstance *mongo.Database) error {
	if events != nil && len(events) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EstateEvent{})

		operations := make([]mongo.WriteModel, len(events))
		for i, estateEvent := range events {
			var filterPayload = bson.M{"collection": estateEvent.Collection, "contract": estateEvent.Contract, "asset_id": estateEvent.AssetId, "event_type": estateEvent.EventType, "transaction": estateEvent.Transaction}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(estateEvent).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}

func getLatestEventTimestamp(collection collections.Collection, dbInstance *mongo.Database) (int64, error) {
	dbCollection := database.CollectionInstance(dbInstance, &EstateEvent{})
	event := &EstateEvent{}
	err := dbCollection.FindOne(context.Background(), bson.M{"collection": string(collection)}, &options.FindOneOptions{Sort: bson.M{"evt_timestamp": -1}}).Decode(event)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return 0, nil
		}
	}
	return event.EvtTimestamp, nil
}
