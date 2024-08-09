package movements

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"encoding/json"
)

func DatabaseTest() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	//str, err := getEstateEventsLogsByTransactionHash(collections.CollectionDcl, "0xb16a029c001884176891c935030ef9410129d6e3baa325d6f3760fe77d35f8ed", dbInstance)
	/*str, err := getCoordinatesOfLandsByIdentifiers(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"),
	[]string{"115792089237316195423570985008687907844082360758775225525946469607255387930577"},
	dbInstance)
	str, err := getDistancesByEstateAssetLands(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"),
		[]string{"-28,-47"},
		dbInstance)*/
	str, err := getAllAloneEstateSaleEvents(collections.CollectionDcl, dbInstance)
	if err != nil {
		panic(err)
	}
	v, err := json.MarshalIndent(len(str), "", "  ")
	println(string(v))
}
