package transactions_logs

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

const TxLogsArguments = "tx_logs"

type TxLogsAddDataGetter struct {
	Collection collections.Collection
}

func (x TxLogsAddDataGetter) FetchData(worker *multithread.Worker) {

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

	worker.LoggingExtra("Fetching Collection Info from database...")
	data, err = getNftCollectionInfo(x.Collection, databaseInstance)
	worker.LoggingExtra("Fetching Collection Info from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type TxLogsMainDataGetter struct {
	Collection collections.Collection
}

func (x TxLogsMainDataGetter) FetchData(worker *multithread.Worker) {

	flag := false

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Get Block number boundaries for topics...")
	boundaries, err := getTopicBoundariesForLogs(x.Collection, databaseInstance)
	if err != nil {
		worker.LoggingError("Failed to Get Latest Block Numbers fetched !", err)
		return
	}
	worker.LoggingExtra("Get Block number boundaries for topics OK!")

	topics := make([]string, 0)
	for s, _ := range boundaries {
		topics = append(topics, s)
	}

	iTopic := 0
	currentTopic := topics[iTopic]
	currentTopicInfo, _ := boundaries[currentTopic]
	currentBN := currentTopicInfo.StartBlock

	worker.LoggingExtra("Start fetching eth events logs !")
	for !flag {

		interrupted := (*worker.ItrChecker)(worker)
		if interrupted {
			worker.LoggingExtra("Break getter loop. Process interrupted !")
			flag = true
		} else {
			worker.LoggingExtra("Getting more data...")

			var data any = nil
			var err0 error = nil

			task := fmt.Sprintf("%s-%d", currentTopic, currentBN)

			response, nextLFBN, err2 := getEthEventsLogsOfTopic(currentTopicInfo, currentBN)
			currentTopicInfo.StartBlock = nextLFBN
			if err2 != nil {
				err0 = err2
			} else {
				data = map[string]any{task: response}
				if len(response) > 0 {
					currentBN = nextLFBN
				} else if iTopic+1 >= len(topics) {
					flag = true
				} else {
					iTopic = iTopic + 1
					currentTopic = topics[iTopic]
					currentTopicInfo, _ = boundaries[currentTopic]
					currentBN = currentTopicInfo.StartBlock
				}
			}

			multithread.PublishDataNotification(worker, task, helpers.AnytiseData(data), err0)
			if err0 != nil {
				worker.LoggingError("Error when getting data !", err0)
				flag = true
			} else {
				worker.LoggingExtra("Sleeping 1s before getting more data...")
				time.Sleep(1 * time.Second)
			}

		}

	}

	multithread.PublishDoneNotification(worker)

}

type TxLogsDataParser struct {
	Collection collections.Collection
}

func (x TxLogsDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
	flag := false

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
						cltInfo := addData.(*collections.CollectionInfo)
						events := mainData.([]*helpers.EthEventLog)

						err := parseTransactionLogs(events, cltInfo, wg)

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

	addDataJob := &TxLogsAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &TxLogsMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &TxLogsDataParser{Collection: collections.Collection(collection)}

	workTitle := "Transaction Logs Synchronizer"
	workerTitles := []string{
		"Collection Info Getter",
		"Eth Events Logs Getter",
		"Eth Events Logs --> Transaction Logs Converter",
	}
	workerDescriptions := []string{
		"Get collection info from database",
		"Get all Eth events logs from Infura API",
		"Convert all Eth events logs to transaction logs and save in database",
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
