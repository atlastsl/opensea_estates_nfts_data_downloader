package eth_events

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

const EthEventsArguments = "eth_events"

type EthEventsAddDataGetter struct {
	Collection collections.Collection
}

func (x EthEventsAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type EthEventsMainDataGetter struct {
	Collection collections.Collection
}

func (x EthEventsMainDataGetter) FetchData(worker *multithread.Worker) {

	flag := false

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Get Block Numbers Slices...")
	bnSlices, err := getSlicesOfBlockNumbers(x.Collection, databaseInstance)
	if err != nil {
		worker.LoggingError("Failed to get block numbers slices !", err)
		return
	}
	worker.LoggingExtra("Get Block Numbers Slices OK!")
	if len(bnSlices) == 0 {
		worker.LoggingExtra("Block Numbers up to date !!")
		return
	}

	iSlice := 0
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

			task := fmt.Sprintf("%d-%d", bnSlices[iSlice][0], bnSlices[iSlice][1])

			response, err2 := getEthEventsLogs(x.Collection, bnSlices[iSlice])
			if err2 != nil {
				err0 = err2
			} else {
				data = map[string]any{task: response}
				iSlice++
				if iSlice >= len(bnSlices) {
					flag = true
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

type EthEventsDataParser struct {
	Collection collections.Collection
}

func (x EthEventsDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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
						mainData := niMap["mainData"]
						events := mainData.([]*EthEventRes)

						err := parseEthEventsRes(events, x.Collection, task, wg)

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

	addDataJob := &EthEventsAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &EthEventsMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &EthEventsDataParser{Collection: collections.Collection(collection)}

	workTitle := "Eth Events Getter"
	workerTitles := []string{
		"[-] Ignored Getter",
		"Eth Events Logs Getter",
		"Eth Events Logs Parser & Saver",
	}
	workerDescriptions := []string{
		"[-] Ignored Getter",
		"Get all Eth events logs from Infura API",
		"Parse, Format and Save in Database all Eth events logs",
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
