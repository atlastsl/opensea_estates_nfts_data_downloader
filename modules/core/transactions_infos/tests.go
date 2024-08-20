package transactions_infos

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"go.mongodb.org/mongo-driver/bson"
)

func TestTransactionInfo() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	dbCollection := database.CollectionInstance(dbInstance, &transactions_hashes.TransactionHash{})
	cursor, err := dbCollection.Find(context.Background(), bson.M{"block_number": 12000058, "collection": "decentraland"})
	if err != nil {
		panic(err)
	}
	defer cursor.Close(context.Background())
	txHashes := make([]*transactions_hashes.TransactionHash, 0)
	err = cursor.All(context.Background(), &txHashes)
	if err != nil {
		panic(err)
	}

	cltInfo := &collections.CollectionInfo{}
	cltInfoCollection := database.CollectionInstance(dbInstance, cltInfo)
	err = cltInfoCollection.FirstWithCtx(context.Background(), bson.M{"name": "decentraland"}, cltInfo)
	if err != nil {
		panic(err)
	}

	err = parseTransactionsInfo(txHashes, cltInfo, nil)
	if err != nil {
		panic(err)
	}
}
