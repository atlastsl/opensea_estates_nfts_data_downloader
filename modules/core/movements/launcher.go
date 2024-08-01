package movements

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"reflect"
	"time"
)

const AssetsMovementsArguments = "movements"

type AssetsMovementsAddDataGetter struct {
	Collection collections.Collection
}

func (x AssetsMovementsAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	multithread.PublishDataNotification(worker, data, err)
	multithread.PublishDoneNotification(worker)
}

type AssetsMovementsMainDataGetter struct {
	Collection collections.Collection
}

func (x AssetsMovementsMainDataGetter) FetchData(worker *multithread.Worker) {

	flag := false
	skip := int64(0)
	limit := int64(100)

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Start fetching Transactions Hashes of Opensea events from database !")
	for !flag {

		interrupted := (*worker.ItrChecker)(worker)
		if interrupted {
			worker.LoggingExtra("Break getter loop. Process interrupted !")
			flag = true
		} else {
			worker.LoggingExtra("Getting more data...")

			var data any = nil
			var err error = nil

			transactions, e0 := getAssetEventsFromDatabase(x.Collection, skip, limit, databaseInstance)
			if e0 != nil {
				err = e0
			} else if transactions == nil || len(transactions) == 0 {
				flag = true
				worker.LoggingExtra("Transactions hashed all fetched ! Break Loop !")
			} else {
				skip = skip + int64(len(transactions))
				data = transactions
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

type AssetsMovementsDataParser struct {
	Collection collections.Collection
}

func (x AssetsMovementsDataParser) ParseData(worker *multithread.Worker) {
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
						transaction := mainData.(string)

						err = parseEstateMovement(x.Collection, transaction, databaseInstance)

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

	addDataJob := &AssetsMovementsAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &AssetsMovementsMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &AssetsMovementsDataParser{Collection: collections.Collection(collection)}

	workTitle := "Assets Movements Parser"
	workerTitles := []string{
		"[-] Ignored Getter",
		"Transactions Hashes Getter",
		"Assets Movements Parser & Writer",
	}
	workerDescriptions := []string{
		"[-] Ignored Getter",
		"Get all transactions Hashes of Opensea In Database",
		"Parse, Format and Save in Database all assets movements",
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
