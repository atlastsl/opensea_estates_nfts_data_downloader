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
	"strings"
)

func usage() {
	log.Println("Usage: strategy [-x collection (Decentraland | TheSandbox)] [-d data (tiles | parcels | estates)] [-c envFilePath]")
	flag.PrintDefaults()
}

func showUsageAndExit(exitCode int) {
	usage()
	os.Exit(exitCode)
}

func readFlags() (*string, *string, *int, bool) {
	var collection = flag.String("x", "", "Collection (Decentraland | TheSandbox)")
	var dataType = flag.String("t", "", "Data Type (tiles | parcels | estates)")
	var nbParsers = flag.Int("n", 1, "Nb Parsers (>0)")
	var envFilePath = flag.String("c", ".env", "Env File Path")
	log.SetFlags(0)
	flag.Usage = usage
	flag.Parse()

	// go run main.go -x decentraland -t tiles -n 1
	// go run main.go -x decentraland -t tiles_distances -n 1
	// go run main.go -x decentraland -t estates_assets -n 1

	if *collection == "" {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	if *dataType == "" {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	if *nbParsers < 0 {
		showUsageAndExit(0)
		return nil, nil, nil, false
	}
	err := godotenv.Load(*envFilePath)
	if err != nil {
		log.Fatalf("Fail to load %s env file", *envFilePath)
		return nil, nil, nil, false
	}

	return collection, dataType, nbParsers, true
}

func main() {
	defer multithread.Recovery()
	collection, dataType, nbParsers, ok := readFlags()
	if !ok {
		os.Exit(0)
	}
	if *dataType == tiles.TileArgument {
		tiles.Launch(strings.ToLower(*collection), *nbParsers)
	} else if *dataType == tiles_distances.TileDistancesArgument {
		tiles_distances.Launch(strings.ToLower(*collection), *nbParsers)
	} else if *dataType == assets.EstatesAssetsArguments {
		assets.Launch(strings.ToLower(*collection), *nbParsers)
	} else if *dataType == eth_events.EthEventsArguments {
		eth_events.Launch(strings.ToLower(*collection), *nbParsers)
	} else if *dataType == ops_events.OpsEventsArguments {
		ops_events.Launch(strings.ToLower(*collection), *nbParsers)
	} else if *dataType == movements.AssetsMovementsArguments {
		movements.Launch(strings.ToLower(*collection), *nbParsers)
	}
}
