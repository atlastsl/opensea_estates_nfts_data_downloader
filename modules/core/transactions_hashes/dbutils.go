package transactions_hashes

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
	"slices"
	"time"
)

func getTopicBoundariesForLogsFromDatabase(metaverse metaverses.MetaverseName, dbInstance *mongo.Database) (map[string]*metaverses.MetaverseInfoLogTopic, error) {
	mtvInfo := &metaverses.MetaverseInfo{}
	dbCollection := database.CollectionInstance(dbInstance, mtvInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": string(metaverse)}, mtvInfo)
	if err != nil {
		return nil, err
	}

	blockchains := make([]string, 0)
	for _, infoAsset := range mtvInfo.Assets {
		if !slices.Contains(blockchains, infoAsset.Blockchain) {
			blockchains = append(blockchains, infoAsset.Blockchain)
		}
	}
	txHashBoundaries := make(map[string]int)
	for _, blockchain := range blockchains {
		lastTxHash := &TransactionHash{}
		txHashCollection := database.CollectionInstance(dbInstance, lastTxHash)
		opts := &options.FindOneOptions{Sort: bson.M{"block_number": -1}}
		err = txHashCollection.FirstWithCtx(context.Background(), bson.M{"metaverse": string(metaverse), "blockchain": blockchain}, lastTxHash, opts)
		if err != nil && !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		txHashBoundaries[blockchain] = lastTxHash.BlockNumber
	}

	result := make(map[string]*metaverses.MetaverseInfoLogTopic)
	for _, topic := range mtvInfo.LogTopics {
		topicName := fmt.Sprintf("%s-%s", topic.Blockchain, topic.Hash)
		if topic.StartBlock == 0 {
			topic.StartBlock = uint64(txHashBoundaries[topic.Blockchain])
		}
		result[topicName] = &topic
	}
	return result, nil
}

func saveTopicBoundariesForLogsInDatabase(metaverse metaverses.MetaverseName, topicsInfo map[string]*metaverses.MetaverseInfoLogTopic, dbInstance *mongo.Database) error {
	mtvInfo := &metaverses.MetaverseInfo{}
	dbCollection := database.CollectionInstance(dbInstance, mtvInfo)
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"metaverse": string(metaverse)}, mtvInfo)
	if err != nil {
		return err
	}
	for _, topic := range mtvInfo.LogTopics {
		topic.StartBlock = topicsInfo[topic.Hash].StartBlock
	}
	err = dbCollection.UpdateWithCtx(context.Background(), mtvInfo)
	return err
}

func saveTransactionHashesInDatabase(transactionHashes []*TransactionHash, dbInstance *mongo.Database) error {
	if transactionHashes != nil && len(transactionHashes) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TransactionHash{})

		operations := make([]mongo.WriteModel, len(transactionHashes))
		for i, txHash := range transactionHashes {
			var filterPayload = bson.M{"metaverse": txHash.Metaverse, "blockchain": txHash.Blockchain, "transaction_hash": txHash.TransactionHash}
			var updatePayload = bson.M{"$set": bson.M{"metaverse": txHash.Metaverse, "blockchain": txHash.Blockchain, "transaction_hash": txHash.TransactionHash, "block_number": txHash.BlockNumber, "block_hash": txHash.BlockHash, "updated_at": time.Now()}}
			operations[i] = mongo.NewUpdateOneModel().SetFilter(filterPayload).SetUpdate(updatePayload).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
