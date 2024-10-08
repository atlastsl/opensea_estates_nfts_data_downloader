package assets

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"os"
	"reflect"
	"sync"
	"time"
)

const EstatesAssetsArguments = "estates_assets"

type EstateAssetAddDataGetter struct {
	Collection collections.Collection
}

func (x EstateAssetAddDataGetter) FetchData(worker *multithread.Worker) {

	var data any = true
	var err error = nil

	databaseInstance, err := database.NewDatabaseConnection()
	if err != nil {
		worker.LoggingError("Failed to connect to database !", err)
		return
	}
	defer database.CloseDatabaseConnection(databaseInstance)

	if x.Collection == collections.CollectionDcl {
		data, err = fetchTileMacroDistances(x.Collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), databaseInstance)
	}

	multithread.PublishDataNotification(worker, "-", data, err)
	multithread.PublishDoneNotification(worker)
}

type EstateAssetMainDataGetter struct {
	Collection collections.Collection
}

func (x EstateAssetMainDataGetter) FetchData(worker *multithread.Worker) {

	flag := false
	nextToken := ""

	for !flag {

		interrupted := (*worker.ItrChecker)(worker)
		if interrupted {
			worker.LoggingExtra("Break getter loop. Process interrupted !")
			flag = true
		} else {
			worker.LoggingExtra("Getting more data...")

			var data any = nil
			var err error = nil

			task := "first"
			if nextToken != "" {
				task = nextToken
			}

			response, err2 := getAssetFromOpensea(x.Collection, nextToken)

			if err2 != nil {
				err = err2
			} else {
				mapData := map[string][]*helpers.OpenseaNftAsset{
					task: response.Nfts,
				}
				if response.Next != nil {
					nextToken = *response.Next
				} else {
					flag = true
				}
				//flag = true
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

type EstateAssetDataParser struct {
	Collection collections.Collection
}

func (x EstateAssetDataParser) ParseData(worker *multithread.Worker, wg *sync.WaitGroup) {
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
						allDistances := addData.([]*tiles_distances.MapTileMacroDistance)
						osAssetInfo := mainData.([]*helpers.OpenseaNftAsset)

						err := parseEstateAssetInfo(osAssetInfo, allDistances, wg)

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

	addDataJob := &EstateAssetAddDataGetter{Collection: collections.Collection(collection)}
	mainDataJob := &EstateAssetMainDataGetter{Collection: collections.Collection(collection)}
	parserJob := &EstateAssetDataParser{Collection: collections.Collection(collection)}

	workTitle := "Map Tiles Distances Calculator"
	workerTitles := []string{
		"[-] Ignored Getter",
		"Opensea Assets Getter",
		"Estate Info Data Parser",
	}
	workerDescriptions := []string{
		"[-] Ignored Getter",
		"Get all Estate Assets from Opensea API",
		"Parse, Format and Save in Database all Estate Assets & Some characterics (for lands)",
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
