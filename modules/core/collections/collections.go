package collections

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo/options"
	"time"
)

type Collection string

const (
	EthereumBlockchain            = "ethereum"
	CollectionDcl      Collection = "decentraland"
)

var DecentralandCollectionInfo = &CollectionInfo{
	Name:       string(CollectionDcl),
	Blockchain: EthereumBlockchain,
	Assets: []CollectionInfoAsset{
		{
			Name:     "land",
			Contract: "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d",
		},
		{
			Name:     "estate",
			Contract: "0x959e104e1a4db6317fa58f8295f586e1a978c297",
		},
	},
	LogTopics: []CollectionInfoLogTopic{
		{
			Name:      "TransferToken",
			Hash:      "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts: []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:  0,
		},
		{
			Name:      "TransferToken",
			Hash:      "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts: []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:  0,
		},
		{
			Name:      "AddLandInEstate",
			Hash:      "0xff0e52667d53255667dc777a00af81038a4646367b0d73d8ee8540ca5b0c9a2e",
			Contracts: []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:  0,
		},
		{
			Name:      "RemoveLandFromEstate",
			Hash:      "0x7932eb5ab0d4d4d172776074ee15d13d708465ff5476902ed15a4965434fcab1",
			Contracts: []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:  0,
		},
	},
}

func SaveInfo(colInfo *CollectionInfo) {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	colInfo.CreatedAt = time.Now()
	colInfo.UpdatedAt = time.Now()
	dbCollection := database.CollectionInstance(dbInstance, &CollectionInfo{})
	opts := &options.ReplaceOptions{}
	_, err = dbCollection.ReplaceOne(context.Background(), bson.M{"name": colInfo.Name, "blockchain": colInfo.Blockchain}, colInfo, opts.SetUpsert(true))
	if err != nil {
		panic(err)
	}
}
