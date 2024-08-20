package transactions_infos

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
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

func getTransactionHashesFromDatabase(collection collections.Collection, dbInstance *mongo.Database) ([]*transactions_hashes.TransactionHash, error) {
	dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	opts := &options.FindOptions{Sort: bson.M{"block_timestamp": 1}}
	cursor, err := dbCollection.Find(context.Background(), bson.M{"collection": string(collection)}, opts.SetLimit(40000))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*transactions_hashes.TransactionHash, 0)
	err = cursor.All(context.Background(), &results)
	if err != nil {
		return nil, err
	}
	return results, nil
}

func saveTransactionsLogsInDatabase(txLogs []*TransactionLog, dbInstance *mongo.Database) error {
	if txLogs != nil && len(txLogs) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TransactionLog{})

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

func saveTransactionsInfosDatabase(txInfos []*TransactionInfo, dbInstance *mongo.Database) error {
	if txInfos != nil && len(txInfos) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TransactionInfo{})

		operations := make([]mongo.WriteModel, len(txInfos))
		for i, txInfo := range txInfos {
			var filterPayload = bson.M{"collection": txInfo.Collection, "transaction_hash": txInfo.TransactionHash}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(txInfo).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
