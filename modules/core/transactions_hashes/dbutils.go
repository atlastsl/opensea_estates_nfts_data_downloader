package transactions_hashes

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"time"
)

func getTopicBoundariesForLogsFromDatabase(collection collections.Collection, dbInstance *mongo.Database) (map[string]*collections.CollectionInfoLogTopic, error) {
	cltInfo := &collections.CollectionInfo{}
	dbCollection := database.CollectionInstance(dbInstance, cltInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": string(collection)}, cltInfo)
	if err != nil {
		return nil, err
	}
	result := make(map[string]*collections.CollectionInfoLogTopic)
	for _, topic := range cltInfo.LogTopics {
		result[topic.Hash] = &topic
	}
	return result, nil
}

func saveTopicBoundariesForLogsInDatabase(collection collections.Collection, topicsInfo map[string]*collections.CollectionInfoLogTopic, dbInstance *mongo.Database) error {
	cltInfo := &collections.CollectionInfo{}
	dbCollection := database.CollectionInstance(dbInstance, cltInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"collection": string(collection)}, cltInfo)
	if err != nil {
		return err
	}
	for _, topic := range cltInfo.LogTopics {
		topic.StartBlock = topicsInfo[topic.Hash].StartBlock
	}
	err = dbCollection.UpdateWithCtx(context.Background(), cltInfo)
	return err
}

func saveTransactionHashesInDatabase(transactionHashes []*TransactionHash, dbInstance *mongo.Database) error {
	if transactionHashes != nil && len(transactionHashes) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TransactionHash{})

		operations := make([]mongo.WriteModel, len(transactionHashes))
		for i, txHash := range transactionHashes {
			var filterPayload = bson.M{"collection": txHash.Collection, "blockchain": txHash.Blockchain, "transaction_hash": txHash.TransactionHash}
			var updatePayload = bson.M{"$set": bson.M{"collection": txHash.Collection, "blockchain": txHash.Blockchain, "transaction_hash": txHash.TransactionHash, "block_number": txHash.BlockNumber, "block_hash": txHash.BlockHash, "updated_at": time.Now()}}
			operations[i] = mongo.NewUpdateOneModel().SetFilter(filterPayload).SetUpdate(updatePayload).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
