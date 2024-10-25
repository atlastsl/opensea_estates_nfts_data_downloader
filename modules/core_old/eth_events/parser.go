package eth_events

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"strconv"
	"strings"
	"sync"
	"time"
)

func dclParseEventTopic(topics []string, topicHexNames, topicNames []string) (string, map[string]any) {
	eventNameHex := topics[0]
	eventName, eventParams := "", map[string]any{}
	if eventNameHex == topicHexNames[0] { //dclTransferHexName
		eventName = topicNames[0]
		eventParams["sender"] = helpers.HexRemoveLeadingZeros(topics[1])
		eventParams["receiver"] = helpers.HexRemoveLeadingZeros(topics[2])
		eventParams["asset"], _ = helpers.HexConvertToString(topics[3])
	} else if eventNameHex == topicHexNames[1] { //dclAddLandHexName
		eventName = topicNames[1]
		eventParams["estate"], _ = helpers.HexConvertToInt(topics[1])
		eventParams["land"], _ = helpers.HexConvertToString(topics[2])
	} else if eventNameHex == topicHexNames[2] { //dclRemoveLandHexName
		eventName = topicNames[2]
		eventParams["estate"], _ = helpers.HexConvertToInt(topics[1])
		eventParams["land"], _ = helpers.HexConvertToString(topics[2])
		eventParams["receiver"] = helpers.HexRemoveLeadingZeros(topics[3])
	} else if eventNameHex == topicHexNames[3] { //dclTransfer0HexName
		eventName = topicNames[3]
		eventParams["sender"] = helpers.HexRemoveLeadingZeros(topics[1])
		eventParams["receiver"] = helpers.HexRemoveLeadingZeros(topics[2])
		eventParams["asset"], _ = helpers.HexConvertToString(topics[3])
	}
	return eventName, eventParams
}

func parseEthEventRes(eventRes *EthEventRes, collection collections.Collection, topicHexNames, topicNames []string) *EthEvent {
	blockNumber, _ := helpers.HexConvertToInt(*eventRes.BlockNumber)
	logIndex, _ := helpers.HexConvertToInt(*eventRes.LogIndex)
	transactionIndex, _ := helpers.HexConvertToInt(*eventRes.TransactionIndex)
	cleanTopics := helpers.ArrayMap(eventRes.Topics, func(t string) (bool, string) {
		return true, helpers.HexRemoveLeadingZeros(t)
	}, true, "")
	event := &EthEvent{}
	event.CreatedAt = time.Now()
	event.UpdatedAt = time.Now()
	event.Collection = string(collection)
	event.Address = *eventRes.Address
	event.EventId = strings.Join(cleanTopics, "-")
	event.BlockHash = *eventRes.BlockHash
	event.BlockNumber = blockNumber
	event.Data = *eventRes.Data
	event.LogIndex = logIndex
	event.Removed = *eventRes.Removed
	if collection == collections.CollectionDcl {
		event.EventName, event.EventParams = dclParseEventTopic(eventRes.Topics, topicHexNames, topicNames)
	}
	event.TransactionHash = *eventRes.TransactionHash
	event.TransactionIndex = transactionIndex
	return event
}

func saveParsedEvents(events []*EthEvent, collection collections.Collection, task string) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveEventsInDatabase(events, dbInstance)

	tmp := strings.Split(task, "-")
	topic := tmp[0]
	latestFetchedBN, _ := strconv.ParseUint(tmp[1], 10, 64)
	if len(events) > 0 {
		latestFetchedBN = uint64(events[len(events)-1].BlockNumber)
	}
	if collection == collections.CollectionDcl {
		err = saveLatestFetchedBlockNumber(collection, EthereumChain, topic, latestFetchedBN, dbInstance)
		if err != nil {
			return err
		}
	}

	return err
}

func parseEthEventsRes(eventRes []*EthEventRes, collection collections.Collection, task string, wg *sync.WaitGroup) error {
	topicHexNames, topicNames := getTopicInfo(collection)
	events := helpers.ArrayMap(eventRes, func(t *EthEventRes) (bool, *EthEvent) {
		return true, parseEthEventRes(t, collection, topicHexNames, topicNames)
	}, false, nil)

	wg.Add(1)
	go func() {
		_ = saveParsedEvents(events, collection, task)
		wg.Done()
	}()

	return nil
}
