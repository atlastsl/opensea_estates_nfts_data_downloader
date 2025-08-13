package blocks_info

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
)

func TestBlocksInfo() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	allBlockNumbers, err := getDistinctBlockNumbersFromDatabase(metaverses.MetaverseDcl, dbInstance)
	if err != nil {
		panic(err)
	}

	//for blockchain, blockNumbers := range allBlockNumbers {
	//	data, err := fetchBlocksTimestamps(blockNumbers, blockchain)
	//	if err != nil {
	//		panic(err)
	//	}
	//	str, _ := json.MarshalIndent(data, "", "  ")
	//	fmt.Println(string(str))
	//}
	helpers.PrettyPrintObject(allBlockNumbers)

}
