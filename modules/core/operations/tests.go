package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
)

func TestOperations() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	collection := collections.CollectionDcl
	additionalData, err := getAdditionalData(collection, dbInstance)
	if err != nil {
		panic(err)
	}

	/*blockNumbers, err := getDistinctBlocksNumbers(string(collection), dbInstance)
	if err != nil {
		panic(err)
	}*/

	transactions, err := getTransactionInfoByBlockNumber(7754211, dbInstance)
	if err != nil {
		panic(err)
	}

	err = parseTransactions(transactions, additionalData, nil, nil)
	if err != nil {
		panic(err)
	}

	println(additionalData, transactions)
}