package ops_events

import (
	"decentraland_data_downloader/modules/app/database"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func saveOpsEventInDatabase(estateEvent *EstateEvent, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, &EstateEvent{})
	existing := &EstateEvent{}
	err := dbCollection.First(bson.M{"collection": estateEvent.Collection, "contract": estateEvent.Contract, "asset_id": estateEvent.AssetId, "event_type": estateEvent.EventType, "transaction": estateEvent.Transaction}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		found = false
	}
	if found {
		estateEvent.ID = existing.ID
		err = dbCollection.Update(estateEvent)
		if err != nil {
			return err
		}
	} else {
		err = dbCollection.Create(estateEvent)
		if err != nil {
			return err
		}
	}
	return nil
}
