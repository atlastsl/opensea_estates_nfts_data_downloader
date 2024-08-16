package main

import (
	"decentraland_data_downloader/modules/core/movements"
	"github.com/joho/godotenv"
)

func main() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}

	movements.DatabaseTest()
}
