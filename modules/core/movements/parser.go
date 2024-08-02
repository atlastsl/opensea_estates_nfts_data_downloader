package movements

import (
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/eth_events"
	"decentraland_data_downloader/modules/core/ops_events"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"reflect"
	"slices"
	"strconv"
	"strings"
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

func dclConvertEthEventsToUpdates(ethEvents []eth_events.EthEvent) (updates []*EstateAssetUpdates) {
	eventNames := strings.Split(os.Getenv("DECENTRALAND_LAND_LOGS_TOPICS_NAMES"), ",")
	landsContract := os.Getenv("DECENTRALAND_LAND_CONTRACT")
	estatesContract := os.Getenv("DECENTRALAND_ESTATE_CONTRACT")

	trEthEvents := helpers.ArrayFilter(ethEvents, func(event eth_events.EthEvent) bool {
		return event.EventName == eventNames[0] //dclTransferHexName
	})

	for _, event := range trEthEvents {
		assetId, sender, receiver := event.EventParams["assetId"].(string), event.EventParams["sender"].(string), event.EventParams["receiver"].(string)
		contract := event.Address
		if contract == landsContract {
			if receiver == estatesContract {
				j := slices.IndexFunc(ethEvents, func(item eth_events.EthEvent) bool {
					land := event.EventParams["land"].(string)
					return item.EventName == eventNames[1] && assetId == land //dclAddLandHexName
				})
				if j >= 0 {
					estate := ethEvents[j].EventParams["estate"].(string)
					estateUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estatesContract, estate)
					estateUpdate.inLands = append(estateUpdate.inLands, assetId)
					landUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landsContract, assetId)
					landUpdate.newOwner = fmt.Sprintf("estate-%s", estate)
					continue
				}
			} else if sender == estatesContract {
				j := slices.IndexFunc(ethEvents, func(item eth_events.EthEvent) bool {
					land := event.EventParams["land"].(string)
					return item.EventName == eventNames[2] && assetId == land //dclAddLandHexName
				})
				if j >= 0 {
					estate := ethEvents[j].EventParams["estate"].(string)
					estateUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estatesContract, estate)
					estateUpdate.outLands = append(estateUpdate.outLands, assetId)
					landUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landsContract, assetId)
					landUpdate.newOwner = receiver
					continue
				}
			}
		}
		assetUpdate := safeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landsContract, assetId)
		assetUpdate.newOwner = receiver
		continue
	}

	return
}

func dclSaveUpdatesItemAsMetadata(updates *EstateAssetUpdates, dbInstance *mongo.Database) error {
	metadataList := make([]*assets.EstateAssetMetadata, 0)
	assetEstate, aeLandsMtd, err := getEstateAsset(updates.collection, updates.contract, updates.identifier, dbInstance)
	if err != nil {
		return err
	}

	if updates.contract == os.Getenv("DECENTRALAND_ESTATE_CONTRACT") {
		if (updates.outLands != nil && len(updates.outLands) > 0) || (updates.inLands != nil && len(updates.inLands) > 0) {
			newLands := make([]string, 0)
			if aeLandsMtd != nil {
				newLands = strings.Split(aeLandsMtd.Value, "|")
			}
			if updates.outLands != nil && len(updates.outLands) > 0 {
				results, err := getLandsCoords(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"), updates.outLands, dbInstance)
				if err != nil {
					return err
				}
				coords := helpers.ArrayMap(results, func(t bson.M) (bool, string) {
					if t["coords"] != nil && reflect.TypeOf(t["coords"]).Kind() == reflect.String {
						return true, t["coords"].(string)
					} else {
						return false, ""
					}
				}, true, "")
				newLands = slices.DeleteFunc(newLands, func(s string) bool {
					return slices.Contains(coords, s)
				})
			}
			if updates.inLands != nil && len(updates.inLands) > 0 {
				results, err := getLandsCoords(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"), updates.inLands, dbInstance)
				if err != nil {
					return err
				}
				coords := helpers.ArrayMap(results, func(t bson.M) (bool, string) {
					if t["coords"] != nil && reflect.TypeOf(t["coords"]).Kind() == reflect.String {
						return true, t["coords"].(string)
					} else {
						return false, ""
					}
				}, true, "")
				newLands = slices.Concat(newLands, coords)
			}

			allMacros, err := getMacros(string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"), dbInstance)
			if err != nil {
				return err
			}
			newLandsTilesSlugs := helpers.ArrayMap(newLands, func(t string) (bool, string) {
				return true, fmt.Sprintf("%s|%s|%s", string(collections.CollectionDcl), os.Getenv("DECENTRALAND_LAND_CONTRACT"), t)
			}, true, "")
			if len(allMacros) > 0 {
				for _, macro := range allMacros {
					distance, err := getDistanceToMacro(&macro, newLandsTilesSlugs, dbInstance)
					if err != nil {
						return err
					}
					distanceValue := 0
					if len(newLandsTilesSlugs) > 0 {
						distanceValue = distance.ManDistance
					}
					if distance != nil {
						metadataList = append(metadataList, &assets.EstateAssetMetadata{
							EstateAssetRef: assetEstate.ID,
							MetadataType:   assets.MetadataTypeDistance,
							DataType:       assets.MetadataDataTypeInteger,
							Name:           assets.DistanceMetadataName(distance),
							DisplayName:    assets.DistanceMetadataDisplayName(distance),
							Value:          strconv.FormatInt(int64(distanceValue), 10),
							MacroType:      distance.MacroType,
							MacroRef:       distance.MacroRef,
						})
					}
				}
			}

			metadataList = append(metadataList, &assets.EstateAssetMetadata{
				EstateAssetRef: assetEstate.ID,
				MetadataType:   assets.MetadataTypeSize,
				DataType:       assets.MetadataDataTypeInteger,
				Name:           assets.MetadataNameSize,
				DisplayName:    assets.MetadataDisNameSize,
				Value:          strconv.FormatInt(int64(len(newLands)), 10),
				UpdateDate:     time.Now(),
			})
			metadataList = append(metadataList, &assets.EstateAssetMetadata{
				EstateAssetRef: assetEstate.ID,
				MetadataType:   assets.MetadataTypeLands,
				DataType:       assets.MetadataDataTypeString,
				Name:           assets.MetadataNameLands,
				DisplayName:    assets.MetadataDisNameLands,
				Value:          strings.Join(newLands, "|"),
				UpdateDate:     time.Now(),
			})
		}
	}

	if updates.newOwner != "" {
		metadataList = append(metadataList, &assets.EstateAssetMetadata{
			EstateAssetRef: assetEstate.ID,
			MetadataType:   assets.MetadataTypeOwner,
			DataType:       assets.MetadataDataTypeString,
			Name:           assets.MetadataNameOwner,
			DisplayName:    assets.MetadataDisNameOwner,
			Value:          updates.newOwner,
			UpdateDate:     time.Now(),
		})
	}

	err = saveMetadataInDatabase(metadataList, dbInstance)
	if err != nil {
		return err
	}

	return nil
}

func filterOpenseaEvents(opsEvents []ops_events.EstateEvent) (filtered []ops_events.EstateEvent) {
	filtered = make([]ops_events.EstateEvent, 0)
	if opsEvents != nil && len(opsEvents) > 0 {
		transfers := helpers.ArrayFilter(opsEvents, func(event ops_events.EstateEvent) bool {
			return event.EventType == "transfer"
		})
		sales := helpers.ArrayFilter(opsEvents, func(event ops_events.EstateEvent) bool {
			return event.EventType == "sale"
		})
		for _, transfer := range transfers {
			relatedSale := slices.IndexFunc(sales, func(sale ops_events.EstateEvent) bool {
				return sale.Transaction == transfer.Transaction && sale.Collection == transfer.Collection && sale.Contract == transfer.Contract && sale.AssetId == transfer.AssetId
			})
			if relatedSale >= 0 {
				filtered = append(filtered, sales[relatedSale])
			} else {
				filtered = append(filtered, transfer)
			}
		}
	}
	return
}

func parseOpenseaEvents(opsEvents []ops_events.EstateEvent, dbInstance *mongo.Database) error {
	movements := make([]*AssetMovement, 0)
	if opsEvents != nil && len(opsEvents) > 0 {
		for _, event := range opsEvents {
			estateAsset, _, err := getEstateAsset(event.Collection, event.Contract, event.AssetId, dbInstance)
			if err != nil {
				return err
			}
			movement := &AssetMovement{
				AssetRef:  estateAsset.ID,
				Movement:  event.EventType,
				TxHash:    event.Transaction,
				Exchange:  event.Exchange,
				Chain:     event.Chain,
				MvtDate:   time.UnixMilli(event.EvtTimestamp * 1000),
				Sender:    event.Sender,
				Recipient: event.Recipient,
				Quantity:  event.Quantity,
				Value:     event.Amount,
				Currency:  event.Currency,
				ValueUsd:  0,
			}
			price, _ := getCurrencyPrice(time.UnixMilli(event.EvtTimestamp*1000), event.Currency, dbInstance)
			if price > 0 {
				movement.ValueUsd = event.Amount * price
			}
			movements = append(movements, movement)
		}
	}
	err := saveMovementsInDatabase(movements, dbInstance)
	return err
}

func parseEstateMovement(collection collections.Collection, transactionHash string, dbInstance *mongo.Database) error {
	ethEvents, err := getEthLogsByTransactionHash(collection, transactionHash, dbInstance)
	if err != nil {
		return err
	}
	estatesUpdates := dclConvertEthEventsToUpdates(ethEvents)
	for _, update := range estatesUpdates {
		err = dclSaveUpdatesItemAsMetadata(update, dbInstance)
		if err != nil {
			return err
		}
	}

	opsEvents, err := getEstateEventsByTransactionHash(collection, transactionHash, dbInstance)
	if err != nil {
		return err
	}
	filteredOpsEvents := filterOpenseaEvents(opsEvents)
	err = parseOpenseaEvents(filteredOpsEvents, dbInstance)
	return err
}
