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
	PolygonBlockchain             = "polygon"
	CollectionDcl      Collection = "decentraland"
	CollectionSmn      Collection = "somnium-space"
	CollectionVxl      Collection = "crypto-voxels"
	CollectionSnd      Collection = "the-sandbox"
)

var (
	Collections = []Collection{CollectionDcl, CollectionSmn, CollectionVxl, CollectionSnd}
)

var DecentralandCollectionInfo = &CollectionInfo{
	Name:       string(CollectionDcl),
	Blockchain: []string{EthereumBlockchain},
	Assets: []CollectionInfoAsset{
		{
			Blockchain: EthereumBlockchain,
			Name:       "land",
			Contract:   "0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d",
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "estate",
			Contract:   "0x959e104e1a4db6317fa58f8295f586e1a978c297",
		},
	},
	LogTopics: []CollectionInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0xf87e31492faf9a91b02ee0deaad50d51d56d5d4d", "0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "AddLandInEstate",
			Hash:       "0xff0e52667d53255667dc777a00af81038a4646367b0d73d8ee8540ca5b0c9a2e",
			Contracts:  []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "RemoveLandFromEstate",
			Hash:       "0x7932eb5ab0d4d4d172776074ee15d13d708465ff5476902ed15a4965434fcab1",
			Contracts:  []string{"0x959e104e1a4db6317fa58f8295f586e1a978c297"},
			EndBlock:   0,
		},
	},
}

var SomniumSpaceCollectionInfo = &CollectionInfo{
	Name:       string(CollectionSmn),
	Blockchain: []string{EthereumBlockchain},
	Assets: []CollectionInfoAsset{
		{
			Blockchain: EthereumBlockchain,
			Name:       "parcel",
			Contract:   "0x913ae503153d9A335398D0785Ba60A2d63dDB4e2",
		},
	},
	LogTopics: []CollectionInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x913ae503153d9A335398D0785Ba60A2d63dDB4e2"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x913ae503153d9A335398D0785Ba60A2d63dDB4e2"},
			EndBlock:   0,
		},
	},
}

var CryptoVoxelsCollectionInfo = &CollectionInfo{
	Name:       string(CollectionVxl),
	Blockchain: []string{EthereumBlockchain},
	Assets: []CollectionInfoAsset{
		{
			Blockchain: EthereumBlockchain,
			Name:       "land",
			Contract:   "0x79986aF15539de2db9A5086382daEdA917A9CF0C",
		},
	},
	LogTopics: []CollectionInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x79986aF15539de2db9A5086382daEdA917A9CF0C"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x79986aF15539de2db9A5086382daEdA917A9CF0C"},
			EndBlock:   0,
		},
	},
}

var TheSandboxCollectionInfo = &CollectionInfo{
	Name:       string(CollectionSnd),
	Blockchain: []string{EthereumBlockchain, PolygonBlockchain},
	Assets: []CollectionInfoAsset{
		{
			Blockchain: EthereumBlockchain,
			Name:       "land",
			Contract:   "0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38",
		},
		{
			Blockchain: PolygonBlockchain,
			Name:       "land",
			Contract:   "0x9d305a42A3975Ee4c1C57555BeD5919889DCE63F",
		},
	},
	LogTopics: []CollectionInfoLogTopic{
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38"},
			EndBlock:   0,
		},
		{
			Blockchain: EthereumBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xd5c97f2e041b2046be3b4337472f05720760a198f4d7d84980b7155eec7cca6f",
			Contracts:  []string{"0x5cc5b05a8a13e3fbdb0bb9fccd98d38e50f90c38"},
			EndBlock:   0,
		},
		{
			Blockchain: PolygonBlockchain,
			Name:       "TransferAsset",
			Hash:       "0xddf252ad1be2c89b69c2b068fc378daa952ba7f163c4a11628f55a4df523b3ef",
			Contracts:  []string{"0x9d305a42A3975Ee4c1C57555BeD5919889DCE63F"},
			EndBlock:   0,
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
	_, err = dbCollection.ReplaceOne(context.Background(), bson.M{"name": colInfo.Name}, colInfo, opts.SetUpsert(true))
	if err != nil {
		panic(err)
	}
}
