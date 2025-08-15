package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
)

func TestOperations() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	metaverse := metaverses.MetaverseDcl
	additionalData, err := getAdditionalData(metaverse, dbInstance)
	if err != nil {
		panic(err)
	}

	//blockNumbers, err := getDistinctBlocksNumbers(string(metaverse), dbInstance)
	//if err != nil {
	//	panic(err)
	//}
	//
	//transactions, err := getTransactionInfoByBlockNumbers(blockNumbers[20000:21000], dbInstance)
	//if err != nil {
	//	panic(err)
	//}

	//println(additionalData, transactions)

	/*println("a")*/

	transactions, err := getTransactionInfoByBlockNumber("ethereum", 5284297, dbInstance)
	if err != nil {
		panic(err)
	}

	err = parseTransactions(transactions, additionalData, nil, nil)
	if err != nil {
		panic(err)
	}

	helpers.PrettyPrintObject(transactions)

	/*println(additionalData, transactions)*/
}
