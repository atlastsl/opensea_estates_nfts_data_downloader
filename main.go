package main

import (
	"decentraland_data_downloader/modules/app/multithread"
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/movements"
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"flag"
	"github.com/joho/godotenv"
	"log"
	"os"
)

func usage() {
	log.Println("Usage: strategy [-x collection (Decentraland | TheSandbox)] [-d data (tiles | parcels | estates)] [-c envFilePath]")
	flag.PrintDefaults()
}

func showUsageAndExit(exitCode int) {
	usage()
	os.Exit(exitCode)
}

func readFlags() (*string, *string, bool) {
	var collection = flag.String("x", "", "Collection (Decentraland | TheSandbox)")
	var data = flag.String("x", "", "Data Type (tiles | parcels | estates)")
	var envFilePath = flag.String("c", ".env", "Env File Path")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	if *collection == "" {
		showUsageAndExit(0)
		return nil, nil, false
	}
	if *data == "" {
		showUsageAndExit(0)
		return nil, nil, false
	}
	err := godotenv.Load(*envFilePath)
	if err != nil {
		log.Fatalf("Fail to load %s env file", *envFilePath)
		return nil, nil, false
	}

	return collection, data, true
}

func main() {
	defer multithread.Recovery()
	collection, dataType, ok := readFlags()
	if !ok {
		os.Exit(0)
	}
	if *dataType == tiles.TileArgument {
		tiles.Launch(*collection, 5)
	} else if *dataType == tiles_distances.TileDistancesArgument {
		tiles_distances.Launch(*collection, 5)
	} else if *dataType == assets.EstatesAssetsArguments {
		assets.Launch(*collection, 5)
	} else if *dataType == eth_events.EthEventsArguments {
		eth_events.Launch(*collection, 5)
	} else if *dataType == ops_events.OpsEventsArguments {
		ops_events.Launch(*collection, 5)
	} else if *dataType == movements.AssetsMovementsArguments {
		movements.Launch(*collection, 5)
	}
}
