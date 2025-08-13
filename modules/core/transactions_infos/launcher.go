package transactions_infos

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"sync"
	"time"
)

const TxInfoArguments = "tx_info"

type TxInfoAddDataGetter struct {
	Metaverse metaverses.MetaverseName
}

func (x TxInfoAddDataGetter) FetchData(worker *multithread.Worker) {

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

	worker.LoggingExtra("Fetching Metaverse Info from database...")
	data, err = getMetaverseInfo(x.Metaverse, databaseInstance)
	worker.LoggingExtra("Fetching Metaverse Info from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type TxInfoMainDataGetter struct {
	Metaverse metaverses.MetaverseName
}

func (x TxInfoMainDataGetter) FetchData(worker *multithread.Worker) {

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Fetching transaction hashes from database...")
	data, err := getTransactionsHashesSlices(x.Metaverse, databaseInstance)
	worker.LoggingExtra("Fetching transaction hashes from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(data), err)
	multithread.PublishDoneNotification(worker)

}

type TxDataParser struct {
	Metaverse metaverses.MetaverseName
}

func (x TxDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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

						err := parseTransactionsInfo(mainData.([]*transactionInput), addData.(*metaverses.MetaverseInfo), wg)

						multithread.PublishTaskDoneNotification(worker, task, err)

					}
				}
			}

		}
	} else {
		worker.LoggingExtra("Next Cursor is Null !!!")
	}
}

func Launch(metaverse string, nbParsers int) {

	addDataJob := &TxInfoAddDataGetter{Metaverse: metaverses.MetaverseName(metaverse)}
	mainDataJob := &TxInfoMainDataGetter{Metaverse: metaverses.MetaverseName(metaverse)}
	parserJob := &TxDataParser{Metaverse: metaverses.MetaverseName(metaverse)}

	workTitle := "Transactions Info Downloader"
	workerTitles := []string{
		"Metaverse Info Getter",
		"Transactions Hashes Getter",
		"Transaction Hash --> Transaction info downloader & Saver",
	}
	workerDescriptions := []string{
		"Get metaverse info from database",
		"Get all transactions hashes from database",
		"Fetch transaction infos from Infura by transaction hash",
	}

	multithread.Launch(
		metaverse,
		addDataJob,
		mainDataJob,
		parserJob,
		nbParsers,
		workTitle,
		workerTitles,
		workerDescriptions,
	)

}
