package operations

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"strconv"
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

	worker.LoggingExtra("Start fetching eth events logs !")
	for _, blockNumber := range blockNumbers {

		interrupted := (*worker.ItrChecker)(worker)
		if interrupted {
			worker.LoggingExtra("Break getter loop. Process interrupted !")
			break
		}

		worker.LoggingExtra("Getting more data...")

		var data any = nil
		var err0 error = nil

		task := strconv.FormatInt(int64(blockNumber), 10)
		transactions, err2 := getTransactionInfoByBlockNumber(string(x.Collection), blockNumber, databaseInstance)
		if err2 != nil {
			err0 = err2
		} else {
			data = map[string]interface{}{
				"tasks": []string{task},
				"data": map[string][]*TransactionFull{
					task: transactions,
				},
			}
		}

		multithread.PublishDataNotification(worker, task, helpers.AnytiseData(data), err0)
		if err0 != nil {
			worker.LoggingError("Error when getting data !", err0)
			break
		} else {
			worker.LoggingExtra("Sleeping 1s before getting more data...")
			time.Sleep(1 * time.Millisecond)
		}

	}

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
