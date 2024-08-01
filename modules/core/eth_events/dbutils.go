package eth_events

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"errors"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func saveEventsInDatabase(events []*EthEvent, dbInstance *mongo.Database) error {
	if events != nil && len(events) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &EthEvent{})
		for _, event := range events {
			existing := &EthEvent{}
			err := dbCollection.First(bson.M{"collection": event.Collection, "address": event.Address, "event_id": event.EventId}, existing)
			found := true
			if err != nil {
				if !errors.Is(err, mongo.ErrNoDocuments) {
					return err
				}
				found = false
			}
			if found {
				event.ID = existing.ID
				err = dbCollection.Update(event)
				if err != nil {
					return err
				}
			} else {
				err = dbCollection.Create(event)
				if err != nil {
					return err
				}
			}
		}
	}
	return nil
}

func getLatestBlock(dbInstance *mongo.Database, collection collections.Collection) (string, error) {
	dbCollection := database.CollectionInstance(dbInstance, &EthEvent{})
	lastRecord := &EthEvent{}
	err := dbCollection.First(bson.M{"collection": string(collection)}, lastRecord, &options.FindOneOptions{Sort: bson.M{"block_number": -1}})
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return "", err
		}
		found = false
	}
	if !found {
		return "earliest", err
	} else {
		return hexutil.EncodeUint64(uint64(lastRecord.BlockNumber)), nil
	}
}
