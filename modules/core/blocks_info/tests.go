package blocks_info

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"encoding/json"
	"fmt"
)

func TestBlocksInfo() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	blockNumbers, err := getDistinctBlockNumbersFromDatabase(collections.CollectionDcl, dbInstance)
	if err != nil {
		panic(err)
	}
	data, err := fetchBlocksTimestamps(blockNumbers)
	if err != nil {
		panic(err)
	}
	str, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(str))
}
