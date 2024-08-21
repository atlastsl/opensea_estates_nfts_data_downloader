package transactions_logs

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func getNftCollectionInfo(collection collections.Collection, dbInstance *mongo.Database) (*collections.CollectionInfo, error) {
	cltInfo := &collections.CollectionInfo{}
	dbCollection := database.CollectionInstance(dbInstance, cltInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": string(collection)}, cltInfo)
	if err != nil {
		return nil, err
	}
	return cltInfo, nil
}

func getTopicBoundariesForLogsFromDatabase(collection collections.Collection, dbInstance *mongo.Database) (map[string]*collections.CollectionInfoLogTopic, error) {
	cltInfo := &collections.CollectionInfo{}
	dbCollection := database.CollectionInstance(dbInstance, cltInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": string(collection)}, cltInfo)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*collections.CollectionInfoLogTopic)
	for _, topic := range cltInfo.LogTopics {
		if topic.Name != "TransferAsset" {
			result[topic.Hash] = &topic
		}
	}
	return result, nil
}

func saveTransactionsLogsInDatabase(txLogs []*transactions_infos.TransactionLog, dbInstance *mongo.Database) error {
	if txLogs != nil && len(txLogs) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &transactions_infos.TransactionLog{})

		operations := make([]mongo.WriteModel, len(txLogs))
		for i, txLog := range txLogs {
			var filterPayload = bson.M{"collection": txLog.Collection, "address": txLog.Address, "transaction_hash": txLog.TransactionHash, "event_id": txLog.EventId}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(txLog).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
