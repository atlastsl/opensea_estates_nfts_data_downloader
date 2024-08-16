package movements

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
)

func DatabaseTest() {
	dbInstance, err := database.NewDatabaseConnection()
	/*if err != nil {
		panic(err)
	}
	r, err := getAllEventsTransactionsHashes(collections.CollectionDcl, dbInstance)
	if err != nil {
		panic(err)
	}
	v, err := json.MarshalIndent(r, "", "  ")
	if err != nil {
		panic(err)
	}
	println(string(v))*/

	//str, err := getEstateEventsLogsByTransactionHash(collections.CollectionDcl, "0xb16a029c001884176891c935030ef9410129d6e3baa325d6f3760fe77d35f8ed", dbInstance)
	/*str, err := getCoordinatesOfLandsByIdentifiers(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"),
	[]string{"115792089237316195423570985008687907844082360758775225525946469607255387930577"},
	dbInstance)
	str, err := getDistancesByEstateAssetLands(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"),
		[]string{"-28,-47"},
		dbInstance)*/
	transactionHash := &TxHash{
		hash:      "0x49037d35bd32fa70dbe7118808e6296e44a30a6c1eae951ae8ee8ed02c8a260c",
		timestamp: 1629912641,
	}
	allPrices, err := getCurrencyPrices(collections.CollectionDcl, dbInstance)
	if err != nil {
		panic(err)
	}
	allAssets, err := getAllEstateAssets(collections.CollectionDcl, dbInstance)
	if err != nil {
		panic(err)
	}
	err = parseEstateMovement(collections.CollectionDcl, allAssets, allPrices, transactionHash, dbInstance, nil)
	if err != nil {
		panic(err)
	}
	//filteredOpsEvents := dclConvertEthEventsToUpdates(opsEvents)
	//v, err := json.MarshalIndent(filteredOpsEvents, "", "  ")
	//println(string(v))
}
