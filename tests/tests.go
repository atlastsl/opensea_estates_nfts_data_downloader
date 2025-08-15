package main

import (
	"decentraland_data_downloader/modules/core/operations"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	//movements.DatabaseTest()
	//blocks_info.TestBlocksInfo()
	//collections.SaveInfo(collections.DecentralandCollectionInfo)
	//transactions_hashes.TestTransactionHashes()
	//transactions_infos.TestTransactionInfo()
	//metaverses.SaveCurrencies()
	//collections.SaveInfo(collections.CryptoVoxelsCollectionInfo)
	//collections.SaveInfo(collections.TheSandboxCollectionInfo)
	operations.TestOperations()

	//metaverses.MetaverseTest()
}
