package metaverses

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/helpers"
	"reflect"
	"sync"
	"time"
)

const MetaverseArgument = "metaverse"

type MetaverseAddDataGetter struct {
	Metaverse MetaverseName
}

func (m MetaverseAddDataGetter) FetchData(worker *multithread.Worker) {
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
	data, err = getterAdditionalData(m.Metaverse, databaseInstance)
	worker.LoggingExtra("Fetching Metaverse Info from database OK. Publishing data...")

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type MetaverseMainDataGetter struct {
	Metaverse MetaverseName
}

func (m MetaverseMainDataGetter) FetchData(worker *multithread.Worker) {

	worker.LoggingExtra("Build requests order (Extra Data + Assets Data)")
	tasks, err := getterRequestsOrder(m.Metaverse)
	if err != nil {
		worker.LoggingError("Failed to get requests order !", err)
		return
	}
	worker.LoggingExtra("Build requests order (Extra Data + Assets Data) OK!")

	worker.LoggingExtra("Build requests order (Extra Data + Assets Data) OK - Publishing data... !")
	multithread.PublishDataNotification(worker, "-", helpers.AnytiseData(tasks), nil)

	multithread.PublishDoneNotification(worker)
}

type MetaverseDataParser struct {
	Metaverse MetaverseName
}

func (x MetaverseDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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

						err = processMetaverseData(task, addData.(map[string]any), x.Metaverse, databaseInstance)

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

	addDataJob := &MetaverseAddDataGetter{Metaverse: MetaverseName(metaverse)}
	mainDataJob := &MetaverseMainDataGetter{Metaverse: MetaverseName(metaverse)}
	parserJob := &MetaverseDataParser{Metaverse: MetaverseName(metaverse)}

	workTitle := "Metaverses Elements Downloader"
	workerTitles := []string{
		"Metaverse Info Getter",
		"Metaverse Extra & Asset Downloader",
		"Metaverse Extra & Asset Parser",
	}
	workerDescriptions := []string{
		"Get or create metaverse info in database",
		"Download from metaverse API all relevant data of metaverse",
		"Parse all relevant data of metaverse and store in database",
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
