package transactions_hashes

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
	"go.mongodb.org/mongo-driver/bson"
)

func TestTransactionHashes() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	defer database.CloseDatabaseConnection(dbInstance)

	metaverseName := metaverses.MetaverseSnd
	boundaries, err := getTopicBoundariesForLogs(metaverseName, dbInstance)
	if err != nil {
		panic(err)
	}

	cltInfo := &metaverses.MetaverseInfo{}
	cltInfoCollection := database.CollectionInstance(dbInstance, cltInfo)
	err = cltInfoCollection.FirstWithCtx(context.Background(), bson.M{"name": string(metaverseName)}, cltInfo)
	if err != nil {
		panic(err)
	}

	topics := make([]string, 0)
	for s, _ := range boundaries {
		topics = append(topics, s)
	}

	iTopic := 2
	currentTopic := topics[iTopic]
	currentTopicInfo, _ := boundaries[currentTopic]
	currentBN := currentTopicInfo.StartBlock

	response, nextLFBN, err2 := getEthEventsLogsOfTopic(metaverseName, currentTopicInfo, currentBN)
	if err2 != nil {
		panic(err2)
	}
	helpers.PrettyPrintObject(response)
	println(nextLFBN)
}
