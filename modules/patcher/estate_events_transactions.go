package patcher

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/ops_events"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

type BlockTimestamp struct {
	mgm.DefaultModel `bson:",inline"`
	BlockNumber      int64     `bson:"block_number,omitempty"`
	Timestamp        time.Time `bson:"timestamp,omitempty"`
}

func getBadEstateEvents(dbInstance *mongo.Database) ([]*ops_events.EstateEvent, error) {
	estateEventsCol := database.CollectionInstance(dbInstance, &ops_events.EstateEvent{})
	ethEventsCol := database.CollectionInstance(dbInstance, &eth_events.EthEvent{})
	bckTimestampsCol := database.CollectionInstance(dbInstance, &BlockTimestamp{})
	cursor, err := estateEventsCol.Find(context.Background(), bson.D{{"transaction", bson.D{{"$exists", false}}}})
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	badEvents := make([]*ops_events.EstateEvent, 0)
	err = cursor.All(context.Background(), &badEvents)
	if err != nil {
		return nil, err
	}
	return badEvents, nil
}

func processEventTxHash(event *ops_events.EstateEvent, dbInstance *mongo.Database) error {

}
