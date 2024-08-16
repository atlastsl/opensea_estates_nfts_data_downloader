package movements

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"sync"
	"time"
)

const AssetsMovementsArguments = "movements"

type AssetsMovementsAddDataGetter struct {
	Collection collections.Collection
}

func (x AssetsMovementsAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connected to database successfully !")

	worker.LoggingExtra("Fetching all estates assets and all currency prices from database...")
	data, err = getAdditionalData(x.Collection, databaseInstance)
	worker.LoggingExtra("Fetching all estates assets and all currency prices from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(data), err)
	multithread.PublishDoneNotification(worker)
}

type AssetsMovementsMainDataGetter struct {
	Collection collections.Collection
}

func (x AssetsMovementsMainDataGetter) FetchData(worker *multithread.Worker) {

	var data any = nil
	var err error = nil

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Fetching transactions hashes from database...")
	if x.Collection == collections.CollectionDcl {
		hashes, transactions, e0 := getAllEventsTransactionsHashes(x.Collection, databaseInstance)
		if e0 != nil {
			err = e0
		} else {
			data = map[string]interface{}{
				"tasks": hashes,
				"data":  transactions,
			}
		}
	}
	worker.LoggingExtra("Fetching transactions hashes from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(data), err)
	multithread.PublishDoneNotification(worker)

}

type AssetsMovementsDataParser struct {
	Collection collections.Collection
}

func (x AssetsMovementsDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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
					if reflect.TypeOf(nextInput).Kind() == reflect.Map {
						niMap := nextInput.(map[string]any)
						mainData := niMap["mainData"]
						addData := niMap["addData"].(map[string]any)
						allAssets := addData["assets"].([]*assets.EstateAsset)
						allPrices := addData["prices"].(map[string][]*CurrencyPrice)
						transaction := mainData.(*TxHash)

						err = parseEstateMovement(x.Collection, allAssets, allPrices, transaction, databaseInstance, wg)

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
