package blocks_info

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"strings"
	"sync"
	"time"
)

const BlocksInfoArguments = "blocks_info"

type BlocksInfoAddDataGetter struct {
	Metaverse metaverses.MetaverseName
}

func (x BlocksInfoAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type BlocksInfoMainDataGetter struct {
	Metaverse metaverses.MetaverseName
}

func (x BlocksInfoMainDataGetter) FetchData(worker *multithread.Worker) {

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connection to database OK!")

	worker.LoggingExtra("Fetching distinct block number from database...")
	data, err := getBlockNumbers(x.Metaverse, databaseInstance)
	worker.LoggingExtra("Fetching distinct block number from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(data), err)
	multithread.PublishDoneNotification(worker)

}

type BlocksInfoDataParser struct {
	Metaverse metaverses.MetaverseName
}

func (x BlocksInfoDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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
						blockNumbers := mainData.([]uint64)
						blockchain := strings.Split(task, "_")[0]

						err := parseBlockTimestamps(blockNumbers, blockchain, x.Metaverse, wg)

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

	addDataJob := &BlocksInfoAddDataGetter{Metaverse: metaverses.MetaverseName(metaverse)}
	mainDataJob := &BlocksInfoMainDataGetter{Metaverse: metaverses.MetaverseName(metaverse)}
	parserJob := &BlocksInfoDataParser{Metaverse: metaverses.MetaverseName(metaverse)}

	workTitle := "Blocks Info (Timestamp) Downloader"
	workerTitles := []string{
		"[-] Ignored Getter",
		"Distinct blocks numbers Getter",
		"Block info Writer",
	}
	workerDescriptions := []string{
		"[-] Ignored Getter",
		"Get distinct blocks number of transaction hashes from database",
		"Save blocks info in transaction hashes documents in database",
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
