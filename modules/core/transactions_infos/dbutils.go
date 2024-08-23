package transactions_infos

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"decentraland_data_downloader/modules/helpers"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"slices"
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

func getTransactionHashesFromDatabase(collection collections.Collection, dbInstance *mongo.Database) ([]*transactionInput, error) {
	tLogsCollection := database.CollectionInstance(dbInstance, &TransactionLog{})
	logsExistingHashes, err := tLogsCollection.Distinct(context.Background(), "transaction_hash", bson.M{})
	if err != nil {
		return nil, err
	}
	logsExistingHashesStr := make([]string, len(logsExistingHashes))
	for i, hash := range logsExistingHashes {
		logsExistingHashesStr[i] = hash.(string)
	}

	tInfoCollection := database.CollectionInstance(dbInstance, &TransactionInfo{})
	infExistingHashes, err := tInfoCollection.Distinct(context.Background(), "transaction_hash", bson.M{})
	if err != nil {
		return nil, err
	}
	infExistingHashesStr := make([]string, len(infExistingHashes))
	for i, hash := range infExistingHashes {
		infExistingHashesStr[i] = hash.(string)
	}

	txHashCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	opts := &options.FindOptions{Sort: bson.M{"block_timestamp": 1}}
	cursor, err := txHashCollection.Find(context.Background(), bson.M{"collection": string(collection), "transaction_hash": bson.M{"$nin": helpers.BSONStringA(logsExistingHashesStr)}}, opts.SetLimit(70000))
	if err != nil {
		return nil, err
	}
	defer cursor.Close(context.Background())
	results := make([]*transactionInput, 0)
	for cursor.Next(context.Background()) {
		txHash := &transactions_hashes.TransactionHash{}
		err = cursor.Decode(txHash)
		if err != nil {
			return nil, err
		}
		result := &transactionInput{txHash: txHash, fetchLogs: true, fetchInfo: !slices.Contains(infExistingHashesStr, txHash.TransactionHash)}
		results = append(results, result)
	}
	return results, nil
}

func saveTransactionsLogsInDatabase(txLogs []*TransactionLog, dbInstance *mongo.Database) error {
	if txLogs != nil && len(txLogs) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TransactionLog{})

		operations := make([]mongo.WriteModel, len(txLogs))
		for i, txLog := range txLogs {
			var filterPayload = bson.M{"address": txLog.Address, "transaction_hash": txLog.TransactionHash, "event_id": txLog.EventId}
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
			var filterPayload = bson.M{"transaction_hash": txInfo.TransactionHash}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(txInfo).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
