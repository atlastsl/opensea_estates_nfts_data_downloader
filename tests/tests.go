package main

import (
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
	//transactions_infos.TestTransactionInfo()
	//collections.SaveCurrencies()
	//collections.SaveInfo(collections.CryptoVoxelsCollectionInfo)
	//collections.SaveInfo(collections.TheSandboxCollectionInfo)
	//operations.TestOperations()
}
