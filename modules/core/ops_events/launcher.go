package ops_events

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"sync"
	"time"
)

const OpsEventsArguments = "ops_events"

type OpsEventsAddDataGetter struct {
	Collection collections.Collection
}

func (x OpsEventsAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type OpsEventsMainDataGetter struct {
	Collection collections.Collection
}

func (x OpsEventsMainDataGetter) FetchData(worker *multithread.Worker) {

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Get latest event timestamp from database...")
	_, err = getLatestEventTimestamp(x.Collection, databaseInstance)
	if err != nil {
		worker.LoggingError("Failed to get latest event timestamp from database !", err)
		return
	}
	worker.LoggingExtra("Get latest event timestamp from database OK!")

	flag := false
	nextToken := "LWV2ZW50X3RpbWVzdGFtcD0yMDIwLTEwLTE1KzE1JTNBMTglM0EzMSYtZXZlbnRfdHlwZT1zdWNjZXNzZnVsJi1waz03NDEwODU2OQ=="

	worker.LoggingExtra("Start fetching Opensea events logs !")
	for !flag {

		interrupted := (*worker.ItrChecker)(worker)
		if interrupted {
			worker.LoggingExtra("Break getter loop. Process interrupted !")
			flag = true
		} else {
			worker.LoggingExtra("Getting more data...")

			var data any = nil
			var err error = nil

			task := nextToken
			if nextToken == "" {
				task = "first"
			}

			response, err2 := getEventsFromOpensea(x.Collection, nextToken)
			if err2 != nil {
				err = err2
			} else {
				/*eventsToSave := helpers.ArrayFilter(response.Events, func(event *helpers.OpenseaNftEvent) bool {
					return int64(*event.EventTimestamp) > latestEvtTimestamp
				})
				eventsToIgnore := helpers.ArrayFilter(response.Events, func(event *helpers.OpenseaNftEvent) bool {
					return int64(*event.EventTimestamp) <= latestEvtTimestamp
				})
				mapData := map[string][]*helpers.OpenseaNftEvent{task: eventsToSave}
				if len(eventsToIgnore) > 0 {
					flag = true
				} else if response.Next == nil {
					flag = true
				} else {
					nextToken = *response.Next
				}*/
				mapData := map[string][]*helpers.OpenseaNftEvent{task: response.Events}
				if response.Next == nil {
					flag = true
				} else {
					nextToken = *response.Next
				}
				data = mapData
			}

			multithread.PublishDataNotification(worker, task, helpers.AnytiseData(data), err)
			if err != nil {
				worker.LoggingError("Error when getting data !", err)
				flag = true
			} else {
				worker.LoggingExtra("Sleeping 1s before getting more data...")
				time.Sleep(1 * time.Second)
			}

		}

	}

	multithread.PublishDoneNotification(worker)

}

type OpsEventsDataParser struct {
	Collection collections.Collection
}

func (x OpsEventsDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
	flag := false

	worker.LoggingExtra("Start parse Opensea events logs !")
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
						estateEvent := mainData.([]*helpers.OpenseaNftEvent)

						err := parseEstateEventInfo(x.Collection, estateEvent, wg)

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

	addDataJob := &OpsEventsAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &OpsEventsMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &OpsEventsDataParser{Collection: collections.Collection(collection)}

	workTitle := "Opensea Events Getter"
	workerTitles := []string{
		"[-] Ignored Getter",
		"Opensea Events Getter",
		"Opensea Events Parser & Saver",
	}
	workerDescriptions := []string{
		"[-] Ignored Getter",
		"Get all Opensea events from OpenseaAPI",
		"Parse, Format and Save in Database all Opensea events",
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
