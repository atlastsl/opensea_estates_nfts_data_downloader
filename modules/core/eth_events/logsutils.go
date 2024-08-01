package eth_events

import (
	"decentraland_data_downloader/modules/core/collections"
	"os"
	"strings"
)

func getTopicInfo(collection collections.Collection) (topicHexNames []string, topicNames []string) {
	topicHexNames = make([]string, 0)
	topicNames = make([]string, 0)
	if collection == collections.CollectionDcl {
		topicHexNames = strings.Split(os.Getenv("DECENTRALAND_LAND_LOGS_TOPICS_HEX"), ",")
		topicNames = strings.Split(os.Getenv("DECENTRALAND_LAND_LOGS_TOPICS_NAMES"), ",")
	}
	return
}

func getAddresses(collection collections.Collection) (addresses []string) {
	addresses = make([]string, 0)
	if collection == collections.CollectionDcl {
		addresses = []string{os.Getenv("DECENTRALAND_LAND_CONTRACT"), os.Getenv("DECENTRALAND_ESTATE_CONTRACT")}
	}
	return
}
