package movements

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/core_old/assets"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
	"math/big"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type EstateAssetUpdates struct {
	collection string
	contract   string
	identifier string
	newOwner   string
	outLands   []string
	inLands    []string
}

func safeGetEthEventLogParam(params map[string]any, paramName string) (string, bool) {
	paramRawVal, ok := params[paramName]
	if ok {
		if reflect.TypeOf(paramRawVal).Kind() == reflect.String {
			return paramRawVal.(string), true
		} else if reflect.TypeOf(paramRawVal).Kind() == reflect.Int {
			return strconv.FormatInt(int64(paramRawVal.(int)), 10), true
		} else if reflect.TypeOf(paramRawVal).Kind() == reflect.Int64 {
			return strconv.FormatInt(paramRawVal.(int64), 10), true
		} else if reflect.TypeOf(paramRawVal).Kind() == reflect.Int32 {
			return strconv.FormatInt(int64(paramRawVal.(int32)), 10), true
		} else if reflect.TypeOf(paramRawVal).Kind() == reflect.Float64 {
			return strconv.FormatFloat(paramRawVal.(float64), 'f', 10, 64), true
		}
	}
	return "", false
}

func safeGetEstateAsset(collection, contract, identifier string, allAssets []*assets.EstateAsset) *assets.EstateAsset {
	index := slices.IndexFunc(allAssets, func(estateAsset *assets.EstateAsset) bool {
		return estateAsset.Collection == collection && estateAsset.Contract == contract && estateAsset.Identifier == identifier
	})
	if index < 0 {
		return nil
	} else {
		return allAssets[index]
	}
}

func safeGetEstateAssetUpdate(updates *[]*EstateAssetUpdates, collection, contract, identifier string) *EstateAssetUpdates {
	i := slices.IndexFunc(*updates, func(item *EstateAssetUpdates) bool {
		return item.collection == collection && item.contract == contract && item.identifier == identifier
	})
	if i >= 0 {
		return (*updates)[i]
	} else {
		newUpdate := &EstateAssetUpdates{collection: collection, contract: contract, identifier: identifier, newOwner: "", outLands: []string{}, inLands: []string{}}
		*updates = append(*updates, newUpdate)
		return newUpdate
	}
}

func safeGetEstateAssetMinDistance(allDistances []*tiles_distances.MapTileMacroDistance, macroType string) *tiles_distances.MapTileMacroDistance {
	result := new(tiles_distances.MapTileMacroDistance)
	result.MacroType = macroType
	if allDistances != nil && len(allDistances) > 0 {
		mtDistances := helpers.ArrayFilter(allDistances, func(distance *tiles_distances.MapTileMacroDistance) bool {
			return distance.MacroType == macroType
		})
		if len(mtDistances) > 0 {
			minDistance := math.MaxInt
			for _, distance := range mtDistances {
				if distance.ManDistance < minDistance {
					result = distance
					minDistance = distance.ManDistance
				}
			}
		}
	}
	return result
}

func safeGetCurrencyPrice(allPrices []*CurrencyPrice, date time.Time) float64 {
	price := 0.0
	if allPrices != nil && len(allPrices) > 0 {
		if date.UnixMilli() < allPrices[0].Start.UnixMilli() {
			price = allPrices[0].Open
		} else if date.UnixMilli() >= allPrices[len(allPrices)-1].End.UnixMilli() {
			price = allPrices[len(allPrices)-1].Close
		} else {
			bestPriceInstance := new(CurrencyPrice)
			for _, priceItem := range allPrices {
				if priceItem.Start.UnixMilli() <= date.UnixMilli() && date.UnixMilli() < priceItem.End.UnixMilli() {
					bestPriceInstance = priceItem
					break
				}
			}
			if bestPriceInstance != nil {
				openP := new(big.Float).SetFloat64(bestPriceInstance.Open)
				closeP := new(big.Float).SetFloat64(bestPriceInstance.Close)
				highP := new(big.Float).SetFloat64(bestPriceInstance.High)
				lowP := new(big.Float).SetFloat64(bestPriceInstance.Low)
				temp := new(big.Float).Add(openP, closeP)
				temp = temp.Add(temp, highP)
				temp = temp.Add(temp, lowP)
				temp = temp.Quo(temp, new(big.Float).SetFloat64(4.0))
				price, _ = temp.Float64()
			}
		}
	}
	return price
}

/*
* Convert eth events logs to updates for estate assets
Inputs :
  - `ethEvents` are eth events logs

Outputs :
  - `updates` stores all updates to be recorded for all estate asset affected to eth events logs input
*/
func dclConvertEthEventsToUpdates(ethEvents []*eth_events.EthEvent) (updates []*EstateAssetUpdates) {
	eventNames := strings.Split(os.Getenv("DECENTRALAND_LAND_LOGS_TOPICS_NAMES"), ",")
	landsContract := os.Getenv("DECENTRALAND_LAND_CONTRACT")
	estatesContract := os.Getenv("DECENTRALAND_ESTATE_CONTRACT")

	// Get all transfers logs as array
	transfersLogsTmp := helpers.ArrayFilter(ethEvents, func(event *eth_events.EthEvent) bool {
		return event.EventName == eventNames[0] || event.EventName == eventNames[3] //dclTransferHexName
	})
	// filter multi-events for one asset
	assetsIds := make([]string, 0)
	for _, t := range transfersLogsTmp {
		assetPar, ok := t.EventParams["asset"]
		if ok && !slices.Contains(assetsIds, assetPar.(string)) {
			assetsIds = append(assetsIds, assetPar.(string))
		}
	}
	transfersLogs := make([]*eth_events.EthEvent, 0)
	for _, assetId := range assetsIds {
		transfersLogsOfAssets := helpers.ArrayFilter(transfersLogsTmp, func(t *eth_events.EthEvent) bool {
			tAssetId, hasAssetId := safeGetEthEventLogParam(t.EventParams, "asset")
			return hasAssetId && tAssetId == assetId
		})
		if len(transfersLogsOfAssets) > 0 {
			if len(transfersLogsOfAssets) > 1 {
				senders := helpers.ArrayMap(transfersLogsOfAssets, func(t *eth_events.EthEvent) (bool, string) {
					tSender, hasSender := safeGetEthEventLogParam(t.EventParams, "sender")
					return hasSender, tSender
				}, false, "")
				receivers := helpers.ArrayMap(transfersLogsOfAssets, func(t *eth_events.EthEvent) (bool, string) {
					tReceiver, hasReceiver := safeGetEthEventLogParam(t.EventParams, "receiver")
					return hasReceiver, tReceiver
				}, false, "")
				fSenders := helpers.ArrayFilter(senders, func(s string) bool {
					return !slices.Contains(receivers, s)
				})
				fReceivers := helpers.ArrayFilter(receivers, func(s string) bool {
					return !slices.Contains(senders, s)
				})
				event := transfersLogsOfAssets[0]
				event.EventParams["sender"] = fSenders[0]
				event.EventParams["receiver"] = fReceivers[0]
				transfersLogs = append(transfersLogs, event)
			} else {
				transfersLogs = append(transfersLogs, transfersLogsOfAssets[0])
			}
		}
	}

	// loop every transfer log
	for _, event := range transfersLogs {
		// get transfer log main info (sender, receiver and asset) as string
		assetId, sender, receiver := event.EventParams["asset"].(string), event.EventParams["sender"].(string), event.EventParams["receiver"].(string)
		// transfer log referred contract (land or estate)
		contract := event.Address
		if contract == landsContract { // referred contract is land
			if receiver == estatesContract { // asset is added in an estate
				// get `AddLandInEstate` event log
				j := slices.IndexFunc(ethEvents, func(item *eth_events.EthEvent) bool {
					land, landExists := safeGetEthEventLogParam(item.EventParams, "land")
					return item.TransactionHash == event.TransactionHash && item.EventName == eventNames[1] && landExists && assetId == land //dclAddLandHexName
				})
				// `AddLandInEstate` event log found
				if j >= 0 {
					// Get estate receiver
					estate, _ := safeGetEthEventLogParam(ethEvents[j].EventParams, "estate")
					// Safe initialize updates for estate to be modified (estate receiver)
					estateUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estatesContract, estate)
					// Record "new land added" update for receiver estate
					estateUpdate.inLands = append(estateUpdate.inLands, assetId)
					// Safe initialize updates for moved land (assetId)
					landUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landsContract, assetId)
					// Record "new owner" update for moved land (assetId)
					landUpdate.newOwner = fmt.Sprintf("estate-%s", estate)
					// continue loop
					continue
				}
			} else if sender == estatesContract { // asset (land) is removed from estate
				// get `RemoveLandFromEstate` event log
				j := slices.IndexFunc(ethEvents, func(item *eth_events.EthEvent) bool {
					land, landExists := safeGetEthEventLogParam(item.EventParams, "land")
					return item.TransactionHash == event.TransactionHash && item.EventName == eventNames[2] && landExists && assetId == land //dclAddLandHexName
				})
				// `RemoveLandFromEstate` event log found
				if j >= 0 {
					// Get estate sender
					estate, _ := safeGetEthEventLogParam(ethEvents[j].EventParams, "estate")
					// Safe initialize updates for estate to be modified (estate sender)
					estateUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estatesContract, estate)
					// Record "land removed" update for sender estate
					estateUpdate.outLands = append(estateUpdate.outLands, assetId)
					// Safe initialize updates for moved land (assetId)
					landUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landsContract, assetId)
					// Record "new owner" update for moved land (assetId)
					landUpdate.newOwner = receiver
					// continue loop
					continue
				}
			}
		}
		// Safe initialize updates for moved land (assetId)
		assetUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), contract, assetId)
		// Record "new owner" update for moved land (assetId)
		assetUpdate.newOwner = receiver
	}

	// return updates
	return
}

/*
* convert updates for estate assets to assets metadata
Inputs :
  - `ethEvents` are eth events logs

Outputs :
  - `updates` stores all updates to be recorded for all estate asset affected to eth events logs input
*/
func dclSaveUpdatesItemAsMetadata(allAssets []*assets.EstateAsset, updates *EstateAssetUpdates, transaction *TxHash) ([]*assets.EstateAssetMetadata, error) {
	dbInstance, err := database.NewDatabaseConnection()
	defer database.CloseDatabaseConnection(dbInstance)
	if err != nil {
		return nil, err
	}

	// instantiate metadata return list
	metadataList := make([]*assets.EstateAssetMetadata, 0)

	// get estate asset related to updates in allAssets list
	assetEstate := safeGetEstateAsset(updates.collection, updates.contract, updates.identifier, allAssets)
	if assetEstate == nil {
		println("{dclSaveUpdatesItemAsMetadata} estate asset not found")
		return metadataList, nil
		//return nil, errors.New("{dclSaveUpdatesItemAsMetadata} estate asset not found")
	}

	// asset related to updates in an estate
	if updates.contract == os.Getenv("DECENTRALAND_ESTATE_CONTRACT") {
		// build metadata for `in lands` and `out lands`
		if (updates.outLands != nil && len(updates.outLands) > 0) || (updates.inLands != nil && len(updates.inLands) > 0) {
			// 1. Update Metadata Lands
			// get estate lands metadata
			aeLandsMtd, err := getMetadataByEstateAsset(assetEstate, assets.MetadataTypeLands, dbInstance)
			if err != nil {
				return nil, err
			}
			// rebuild estate lands metadata value
			newLands := make([]string, 0)
			if aeLandsMtd != nil {
				newLands = strings.Split(aeLandsMtd.Value, "|")
			}
			// we must remove some lands
			if updates.outLands != nil && len(updates.outLands) > 0 {
				// Get coordinates of lands to remove from estate
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), updates.outLands, dbInstance)
				if err != nil {
					return nil, err
				}
				// remove lands from estate lands metadata value
				newLands = slices.DeleteFunc(newLands, func(s string) bool {
					return slices.Contains(coords, s)
				})
			}
			// new must add some lands
			if updates.inLands != nil && len(updates.inLands) > 0 {
				// Get coordinates of lands to add to estate
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), updates.inLands, dbInstance)
				if err != nil {
					return nil, err
				}
				// add lands to estate lands metadata value
				newLands = slices.Concat(newLands, coords)
			}
			// update metadata lands content
			newLandsMtd := &assets.EstateAssetMetadata{
				EstateAssetRef: assetEstate.ID,
				MetadataType:   assets.MetadataTypeLands,
				DataType:       assets.MetadataDataTypeString,
				Name:           assets.MetadataNameLands,
				DisplayName:    assets.MetadataDisNameLands,
				Value:          strings.Join(newLands, "|"),
				UpdateDate:     time.UnixMilli(transaction.timestamp * 1000),
			}
			newLandsMtd.CreatedAt = time.Now()
			newLandsMtd.UpdatedAt = time.Now()
			metadataList = append(metadataList, newLandsMtd)
			// update metadata size
			newSizeMtd := &assets.EstateAssetMetadata{
				EstateAssetRef: assetEstate.ID,
				MetadataType:   assets.MetadataTypeSize,
				DataType:       assets.MetadataDataTypeInteger,
				Name:           assets.MetadataNameSize,
				DisplayName:    assets.MetadataDisNameSize,
				Value:          strconv.FormatInt(int64(len(newLands)), 10),
				UpdateDate:     time.UnixMilli(transaction.timestamp * 1000),
			}
			newSizeMtd.CreatedAt = time.Now()
			newSizeMtd.UpdatedAt = time.Now()
			metadataList = append(metadataList, newSizeMtd)

			// 2. Update Metadata Distances
			// Get all distances for new lands
			allDistances, err := getDistancesByEstateAssetLands(updates.collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), newLands, dbInstance)
			if err != nil {
				return nil, err
			}
			// focused distance macro types
			macroTypes := []string{"district", "plaza", "road"}
			// Get minimal distance for all macro types
			for _, macroType := range macroTypes {
				distance := safeGetEstateAssetMinDistance(allDistances, macroType)
				if distance != nil {
					newDistanceMtd := &assets.EstateAssetMetadata{
						EstateAssetRef: assetEstate.ID,
						MetadataType:   assets.MetadataTypeDistance,
						DataType:       assets.MetadataDataTypeInteger,
						Name:           assets.DistanceMetadataName(distance),
						DisplayName:    assets.DistanceMetadataDisplayName(distance),
						Value:          strconv.FormatInt(int64(distance.ManDistance), 10),
						MacroType:      distance.MacroType,
						MacroRef:       distance.MacroRef,
						UpdateDate:     time.UnixMilli(transaction.timestamp * 1000),
					}
					newDistanceMtd.CreatedAt = time.Now()
					newDistanceMtd.UpdatedAt = time.Now()
					metadataList = append(metadataList, newDistanceMtd)
				}
			}

		}
	}

	// 2. Update Metadata Owner
	if updates.newOwner != "" {
		newOwnerMtd := &assets.EstateAssetMetadata{
			EstateAssetRef: assetEstate.ID,
			MetadataType:   assets.MetadataTypeOwner,
			DataType:       assets.MetadataDataTypeAddress,
			Name:           assets.MetadataNameOwner,
			DisplayName:    assets.MetadataDisNameOwner,
			Value:          updates.newOwner,
			UpdateDate:     time.UnixMilli(transaction.timestamp * 1000),
		}
		newOwnerMtd.CreatedAt = time.Now()
		newOwnerMtd.UpdatedAt = time.Now()
		metadataList = append(metadataList, newOwnerMtd)
	}

	return metadataList, nil
}

/**
* Save built estate assets metadata in database
 */
func dclSaveUpdatesAsMetadata(allAssets []*assets.EstateAsset, updates []*EstateAssetUpdates, transaction *TxHash) ([]*assets.EstateAssetMetadata, error) {
	var wLocker sync.RWMutex
	allMetadata := make([]*assets.EstateAssetMetadata, 0)
	allErrors := make([]error, 0)

	var wg = &sync.WaitGroup{}
	for _, updateItem := range updates {
		wg.Add(1)
		go func() {
			metadataListI, err := dclSaveUpdatesItemAsMetadata(allAssets, updateItem, transaction)
			wLocker.Lock()
			if err != nil {
				allErrors = append(allErrors, err)
			} else {
				allMetadata = append(allMetadata, metadataListI...)
			}
			wLocker.Unlock()
			wg.Done()
		}()
	}
	wg.Wait()

	if len(allErrors) > 0 {
		return nil, allErrors[0]
	}

	/*allMetadata := make([]*assets.EstateAssetMetadata, 0)
	for _, updateItem := range updates {
		metadataListI, err := dclSaveUpdatesItemAsMetadata(allAssets, updateItem, transaction)
		if err != nil {
			return nil, err
		}
		allMetadata = append(allMetadata, metadataListI...)
	}*/

	return allMetadata, nil
}

/**
* Filter Opensea Events (Take either transfer or sale)
 */
func parseOpenseaEvents(allAssets []*assets.EstateAsset, allPrices map[string][]*CurrencyPrice, opsEvents []*ops_events.EstateEvent) ([]*AssetMovement, error) {
	movements := make([]*AssetMovement, 0)
	if opsEvents != nil && len(opsEvents) > 0 {
		for _, event := range opsEvents {
			// get estate asset related to updates in allAssets list
			estateAsset := safeGetEstateAsset(event.Collection, event.Contract, event.AssetId, allAssets)
			/*if estateAsset == nil {
				return nil, errors.New("{parseOpenseaEvents} estate asset not found")
			}*/
			amount := event.Amount
			if event.CCyDecimals > 0 {
				bgAmt, decim := big.NewFloat(amount), new(big.Int)
				decim.Exp(big.NewInt(10), big.NewInt(event.CCyDecimals), nil)
				bgAmt.Quo(bgAmt, new(big.Float).SetInt(decim))
				amount, _ = bgAmt.Float64()
			}

			movement := &AssetMovement{
				AssetCollection: event.Collection,
				AssetContract:   event.Contract,
				AssetIdentifier: event.AssetId,
				Movement:        event.EventType,
				TxHash:          getOpsEventTransactionHash(event),
				Exchange:        event.Exchange,
				Chain:           event.Chain,
				MvtDate:         time.UnixMilli(event.EvtTimestamp * 1000),
				Sender:          event.Sender,
				Recipient:       event.Recipient,
				Quantity:        event.Quantity,
				Value:           amount,
				Currency:        event.Currency,
				ValueUsd:        0,
			}
			if estateAsset != nil {
				movement.AssetRef = estateAsset.ID
			}
			movement.CreatedAt = time.Now()
			movement.UpdatedAt = time.Now()
			currencyPrices, hasCp := allPrices[event.Currency]
			if hasCp && amount > 0 {
				price := safeGetCurrencyPrice(currencyPrices, time.UnixMilli(event.EvtTimestamp*1000))
				if price > 0 {
					movement.ValueUsd = amount * price
					movement.CcyPrice = price
				}
			}
			movements = append(movements, movement)
		}
	}
	/*v, _ := json.MarshalIndent(opsEvents, "", "  ")
	println(string(v))
	s, _ := json.MarshalIndent(movements, "", "  ")
	println(string(s))*/
	return movements, nil
}

func saveInDatabase(allMetadata []*assets.EstateAssetMetadata, movements []*AssetMovement, dbInstance *mongo.Database) error {
	/*dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)*/

	err := saveMetadataInDatabase(allMetadata, dbInstance)
	if err != nil {
		return err
	}
	err = saveMovementsInDatabase(movements, dbInstance)
	if err != nil {
		return err
	}

	return nil
}

func parseEstateMovement(collection collections.Collection, allAssets []*assets.EstateAsset, allPrices map[string][]*CurrencyPrice, transaction *TxHash, dbInstance *mongo.Database, _ *sync.WaitGroup) error {
	ethEvents, err := getEthEventsLogsByTransactionHash(collection, transaction.hash, dbInstance)
	if err != nil {
		return err
	}
	estatesUpdates := dclConvertEthEventsToUpdates(ethEvents)
	allMetadata, err := dclSaveUpdatesAsMetadata(allAssets, estatesUpdates, transaction)
	if err != nil {
		return err
	}

	opsEvents, err := getEstateEventsLogsByTransactionHash(collection, transaction.hash, dbInstance)
	if err != nil {
		return err
	}
	filteredOpsEvents := cleanTxHashOpsEvents(opsEvents)
	movements, err := parseOpenseaEvents(allAssets, allPrices, filteredOpsEvents)
	if err != nil {
		return err
	}

	println(allMetadata, movements)
	/*wg.Add(1)
	go func() {
		_ = saveInDatabase(allMetadata, movements, dbInstance)
		wg.Done()
	}()*/
	//err = saveInDatabase(allMetadata, movements, dbInstance)

	return err
}
