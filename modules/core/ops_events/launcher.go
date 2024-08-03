package ops_events

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

const OpsEventsArguments = "ops_events"

type OpsEventsAddDataGetter struct {
	Collection collections.Collection
}

func (x OpsEventsAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	multithread.PublishDataNotification(worker, data, err)
	multithread.PublishDoneNotification(worker)
}

type OpsEventsMainDataGetter struct {
	Collection collections.Collection
}

func (x OpsEventsMainDataGetter) FetchData(worker *multithread.Worker) {

	flag := false
	nextToken := ""

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

			response, err2 := getEventsFromOpensea(x.Collection, nextToken)
			if err2 != nil {
				err = err2
			} else {
				mapData := make(map[string]*helpers.OpenseaNftEvent)
				if response.Events != nil {
					for _, event := range response.Events {
						if event.Transaction != nil && event.EventType != nil && event.Nft != nil && event.Nft.Identifier != nil {
							key := fmt.Sprintf("%s-%s-%s", *event.Transaction, *event.EventType, *event.Nft.Identifier)
							mapData[key] = event
						}
					}
				}
				if response.Next != nil {
					nextToken = *response.Next
				} else {
					flag = true
				}
				data = mapData
			}

			multithread.PublishDataNotification(worker, data, err)
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

func (x OpsEventsDataParser) ParseData(worker *multithread.Worker, _ *sync.WaitGroup) {
	flag := false

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

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
					if reflect.TypeOf(nextInput).Kind() == reflect.String {
						niMap := nextInput.(map[string]any)
						mainData := niMap["mainData"]
						estateEvent := mainData.(*helpers.OpenseaNftEvent)

						err = parseEstateEventInfo(estateEvent, databaseInstance)

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
