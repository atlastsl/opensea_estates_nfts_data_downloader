package tiles_distances

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"os"
	"reflect"
	"sync"
	"time"
)

const TileDistancesArgument = "tiles_distances"

type TilesDistanceAddDataGetter struct {
	Collection collections.Collection
}

func (x TilesDistanceAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = nil
	var err error = nil

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connected to database successfully !")

	worker.LoggingExtra("Fetching Macro data from database...")
	if x.Collection == collections.CollectionDcl {
		data, err = getMacroFromDatabase(x.Collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), databaseInstance)
	}
	worker.LoggingExtra("Macro data fetched from database. Publishing data...")

	multithread.PublishDataNotification(worker, data, err)
	multithread.PublishDoneNotification(worker)
}

type TilesDistanceMainDataGetter struct {
	Collection collections.Collection
}

func (x TilesDistanceMainDataGetter) FetchData(worker *multithread.Worker) {

	var data any = nil
	var err error = nil

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connected to database successfully !")

	worker.LoggingExtra("Fetching Tiles Ids from database...")
	if x.Collection == collections.CollectionDcl {
		data, err = getTilesToWorkFromDatabase(x.Collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), databaseInstance)
	}
	worker.LoggingExtra("Tiles Ids fetched from database. Publishing data...")

	multithread.PublishDataNotification(worker, helpers.AnytiseData(data), err)
	multithread.PublishDoneNotification(worker)
}

type TilesDistanceDistanceCalc struct {
	Collection collections.Collection
}

func (x TilesDistanceDistanceCalc) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
	flag := false

	worker.LoggingExtra("Connecting to database...")
	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)
	worker.LoggingExtra("Connected to database successfully !")

	worker.LoggingExtra("Starting parser loop...")
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
						addData, mainData := niMap["addData"], niMap["mainData"]

						err = nil
						if x.Collection == collections.CollectionDcl {
							err = dclCalculateTileDistances(addData, mainData, databaseInstance, wg)
						}

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

	addDataJob := &TilesDistanceAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &TilesDistanceMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &TilesDistanceDistanceCalc{Collection: collections.Collection(collection)}

	workTitle := "Map Tiles Distances Calculator"
	workerTitles := []string{
		"Macro Data Getter",
		"Tiles Data Getter",
		"Distances Calculator",
	}
	workerDescriptions := []string{
		"Get all Macro from Database",
		"Get all Tiles from Database",
		"Calculate distances between tiles and macros and save in Database",
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
