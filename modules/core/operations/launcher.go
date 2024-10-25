package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
	"reflect"
	"sync"
	"time"
)

const OperationArgument = "operations"

type OperationAddDataGetter struct {
	Collection collections.Collection
}

func (x OperationAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Fetching Additional Data from database...")
	data, err = getAdditionalData(x.Collection, databaseInstance)
	worker.LoggingExtra("Fetching Additional Data from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type OperationMainDataGetter struct {
	Collection collections.Collection
}

func (x OperationMainDataGetter) FetchData(worker *multithread.Worker) {

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Get Distinct block numbers of transactions...")
	blockNumbers, err := getDistinctBlocksNumbers(string(x.Collection), databaseInstance)
	if err != nil {
		worker.LoggingError("Failed to Get Distinct block numbers of transactions !", err)
		return
	}
	worker.LoggingExtra("Get Distinct block numbers of transactions OK!")

	worker.LoggingExtra("Start getting transaction info & logs !")
	var data any = nil
	tasks := make([]string, len(blockNumbers))
	for i, blockNumber := range blockNumbers {
		tasks[i] = fmt.Sprintf("%s_%d", blockNumber.Blockchain, blockNumber.BlockNumber)
	}
	transactions, err2 := getTransactionInfoByBlockNumbers(blockNumbers, databaseInstance)
	if err2 != nil {
		err = err2
	} else {
		data = map[string]interface{}{
			"tasks": tasks,
			"data":  transactions,
		}
	}
	worker.LoggingExtra("Getting transaction info & logs OK - Publishing data... !")
	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(data), err)

	multithread.PublishDoneNotification(worker)

}

type OperationDataParser struct {
	Collection collections.Collection
}

func (x OperationDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
	flag := false

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	if worker.NextCursor != nil {
		for !flag {

			interrupted := (*worker.ItrChecker)(worker)
			if interrupted {
				flag = true
			} else {
				shouldWaitMoreData, task, nextInput := (*worker.NextCursor)(worker)
				if shouldWaitMoreData {
					time.Sleep(time.Second)
				} else if task == "" {
					flag = true
				} else if nextInput != nil {
					if reflect.TypeOf(nextInput).Kind() == reflect.Map {
						niMap := nextInput.(map[string]any)
						addData := niMap["addData"]
						mainData := niMap["mainData"]

						err = parseTransactions(mainData.([]*TransactionFull), addData.(map[string]any), databaseInstance, wg)

						multithread.PublishTaskDoneNotification(worker, task, err)

					}
				}
			}

		}
	} else {
		worker.LoggingExtra("Next Cursor is Null !!!")
	}
}

func Launch(collection string, nbParsers int) {

	addDataJob := &OperationAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &OperationMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &OperationDataParser{Collection: collections.Collection(collection)}

	workTitle := "Operations History Builder"
	workerTitles := []string{
		"Additional Data Getter",
		"Transaction Infos et Logs Getter",
		"Transaction Infos et Logs --> Operations, Assets & Metadata history converter",
	}
	workerDescriptions := []string{
		"Get collection info, currencies data & tiles distances from database",
		"Get transactions infos & logs from database",
		"Process transaction infos & logs to calculate operations history & assets data history",
	}

	multithread.Launch(
		collections.Collection(collection),
		addDataJob,
		mainDataJob,
		parserJob,
		nbParsers,
		workTitle,
		workerTitles,
		workerDescriptions,
	)

}
