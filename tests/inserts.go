package main

import (
	"decentraland_data_downloader/modules/core/collections"
	"github.com/joho/godotenv"
)

func main0() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}
	collections.SaveInfo(collections.DecentralandCollectionInfo)
}
