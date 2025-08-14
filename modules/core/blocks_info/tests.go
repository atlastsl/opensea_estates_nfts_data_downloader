package blocks_info

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
	"strings"
)

func TestBlocksInfo() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	//allBlockNumbers, err := getDistinctBlockNumbersFromDatabase(metaverses.MetaverseDcl, dbInstance)
	allBlockNumbers, err := getBlockNumbers(metaverses.MetaverseSnd, dbInstance)
	if err != nil {
		panic(err)
	}

	keys := helpers.MapGetKeys(allBlockNumbers)
	firstData, _ := allBlockNumbers[keys[0]]
	firstBlockchain := strings.Split(keys[0], "_")[0]
	data, err := fetchBlocksTimestamps(firstData, firstBlockchain)
	if err != nil {
		panic(err)
	}
	///str, _ := json.MarshalIndent(data, "", "  ")
	//fmt.Println(string(str))

	//for blockchain, blockNumbers := range allBlockNumbers {
	//	if len(blockNumbers) > 0 {
	//		data, err := fetchBlocksTimestamps(blockNumbers, blockchain)
	//		if err != nil {
	//			panic(err)
	//		}
	//		str, _ := json.MarshalIndent(data, "", "  ")
	//		fmt.Println(string(str))
	//	}
	//}
	helpers.PrettyPrintObject(data)

}
