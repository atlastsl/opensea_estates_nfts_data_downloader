package metaverses

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/helpers"
)

func MetaverseTest() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	metaverse := MetaverseSmn
	additionalData, err := getterAdditionalData(metaverse, dbInstance)
	if err != nil {
		panic(err)
	}
	tasks, err := getterRequestsOrder(metaverse)
	if err != nil {
		panic(err)
	}
	helpers.PrettyPrintObject(tasks)

	//e1 := processMetaverseData(tasks[0], additionalData, metaverse, dbInstance)
	//if e1 != nil {
	//	panic(e1)
	//}
	//e2 := processMetaverseData(tasks[0], additionalData, metaverse, dbInstance)
	//if e2 != nil {
	//	panic(e2)
	//}
	e3 := processMetaverseData(tasks[len(tasks)-1], additionalData, metaverse, dbInstance)
	if e3 != nil {
		panic(e3)
	}

	println(metaverse)
	helpers.PrettyPrintObject(additionalData)
}
