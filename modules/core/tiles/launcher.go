package tiles

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"reflect"
	"time"
)

const TileArgument = "tiles"

type MapTileAddDataGetter struct {
	Collection collections.Collection
}

func (d *MapTileAddDataGetter) FetchData(worker *multithread.Worker) {
	var data any = nil
	var err error = nil

	if d.Collection == collections.CollectionDcl {
		data, err = getDclDistrictData()
	}

	multithread.PublishDataNotification(worker, data, err)
	multithread.PublishDoneNotification(worker)
}

type MapTileMainDataGetter struct {
	Collection collections.Collection
}

func (d MapTileMainDataGetter) FetchData(worker *multithread.Worker) {
	var data any = nil
	var err error = nil

	if d.Collection == collections.CollectionDcl {
		data, err = getDclTilesData()
	}

	multithread.PublishDataNotification(worker, data, err)
	multithread.PublishDoneNotification(worker)
}

type MapTileParser struct {
	Collection collections.Collection
}

func (m MapTileParser) ParseData(worker *multithread.Worker) {
	flag := false

	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)

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
						if m.Collection == collections.CollectionDcl {
							err = parseDclTileInfo(m.Collection, addData, mainData, task, databaseInstance)
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

	addDataJob := &MapTileAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &MapTileMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &MapTileParser{Collection: collections.Collection(collection)}

	workTitle := "Map Tiles Builder"
	workerTitles := []string{
		"Districts Data Getter",
		"Tiles Data Getter",
		"Map Tiles and Macro Builder",
	}
	workerDescriptions := []string{
		"Get Data about Districts",
		"Get all Data about Map Tiles",
		"Parse, Format and Save in Database all Map Tiles & Macro infos",
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
