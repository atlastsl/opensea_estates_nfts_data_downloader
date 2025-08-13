package operations

import (
	"decentraland_data_downloader/modules/core/metaverses"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"slices"
	"strconv"
	"strings"
)

//func dclGetDistanceByLandsCoords(coords []string, focalZones []*MapFocalZone) []*MapFocalZoneDistance {
//	if focalZones != nil && len(focalZones) > 0 {
//		filteredDistances := make([]*MapFocalZoneDistance, len(focalZones))
//		for i, focalZone := range focalZones {
//			minD := math.MaxInt
//			minDistance := new(MapFocalZoneDistance)
//			if len(coords) > 0 {
//				for _, coord := range coords {
//					cDistance := calculateDistanceToFocalZone2(coord, focalZone)
//					if cDistance.ManDis < minD {
//						minD = cDistance.ManDis
//						minDistance = cDistance
//					}
//				}
//			} else {
//				minDistance = &MapFocalZoneDistance{
//					FocalZone: focalZone,
//					ManDis:    -1,
//				}
//			}
//			filteredDistances[i] = minDistance
//		}
//		return filteredDistances
//	}
//	return make([]*MapFocalZoneDistance, 0)
//}

func dclSafeGetEstateAssetUpdate(updates *[]*assetUpdate, metaverse, contract, identifier string) *assetUpdate {
	i := slices.IndexFunc(*updates, func(item *assetUpdate) bool {
		return item.metaverse == metaverse && item.contract == contract && item.identifier == identifier
	})
	if i >= 0 {
		return (*updates)[i]
	} else {
		newUpdate := &assetUpdate{metaverse: metaverse, contract: contract, identifier: identifier, newOwner: "", outLands: []string{}, inLands: []string{}, operations: []primitive.ObjectID{}}
		*updates = append(*updates, newUpdate)
		return newUpdate
	}
}

func dclConvertTxLogsToAssetUpdates(txLogsInfos []*TransactionLogInfo, mvtInfo *metaverses.MetaverseInfo) (updates []*assetUpdate) {
	landInfo := mvtInfo.GetAsset("land")
	estateInfo := mvtInfo.GetAsset("estate")
	addLandTopic := os.Getenv("ETH_TRANSFER_LOG_DCL_ADD_LAND")
	removeLandTopic := os.Getenv("ETH_TRANSFER_LOG_DCL_RMV_LAND")

	transfersLogsInfos := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetTransfers)
	interAstLogsInfos := filterTransactionLogsInfo(txLogsInfos, filterTxLogsInfoColAssetInter)
	writeIDSInTransferLogs(transfersLogsInfos)

	// loop every transfer log
	for _, logInfo := range transfersLogsInfos {
		// get transfer log main info (sender, receiver and asset) as string
		assetId, sender, receiver := logInfo.Asset, logInfo.From, logInfo.To
		// transfer log referred contract (land or estate)
		contract := logInfo.TransactionLog.Address
		if contract == landInfo.Contract { // referred contract is land
			if receiver == estateInfo.Contract { // asset is added in an estate
				// get `AddLandInEstate` event log
				j := slices.IndexFunc(interAstLogsInfos, func(item *TransactionLogInfo) bool {
					land, landExists := "", false
					if item.Land != "" {
						land = item.Land
						landExists = true
					}
					return item.TransactionLog.TransactionHash == logInfo.TransactionLog.TransactionHash && item.EventName == addLandTopic && landExists && assetId == land //dclAddLandHexName
				})
				// `AddLandInEstate` event log found
				if j >= 0 {
					// Get estate receiver
					estate := interAstLogsInfos[j].Estate
					// Safe initialize updates for estate to be modified (estate receiver)
					estateUpdate := dclSafeGetEstateAssetUpdate(&updates, string(metaverses.MetaverseDcl), estateInfo.Contract, estate)
					// Record "new land added" update for receiver estate
					estateUpdate.inLands = append(estateUpdate.inLands, assetId)
					// Record "operation" which make updates
					estateUpdate.operations = append(estateUpdate.operations, logInfo.TransactionLog.ID)
					// Safe initialize updates for moved land (assetId)
					landUpdate := dclSafeGetEstateAssetUpdate(&updates, string(metaverses.MetaverseDcl), landInfo.Contract, assetId)
					// Record "new owner" update for moved land (assetId)
					landUpdate.newOwner = fmt.Sprintf("estate-%s", estate)
					// Record "operation" which make updates
					landUpdate.operations = append(landUpdate.operations, logInfo.TransactionLog.ID)
					// continue loop
					continue
				}
			} else if sender == estateInfo.Contract { // asset (land) is removed from estate
				// get `RemoveLandFromEstate` event log
				j := slices.IndexFunc(interAstLogsInfos, func(item *TransactionLogInfo) bool {
					land, landExists := "", false
					if item.Land != "" {
						land = item.Land
						landExists = true
					}
					return item.TransactionLog.TransactionHash == logInfo.TransactionLog.TransactionHash && item.EventName == removeLandTopic && landExists && assetId == land //dclAddLandHexName
				})
				// `RemoveLandFromEstate` event log found
				if j >= 0 {
					// Get estate sender
					estate := interAstLogsInfos[j].Estate
					// Safe initialize updates for estate to be modified (estate sender)
					estateUpdate := dclSafeGetEstateAssetUpdate(&updates, string(metaverses.MetaverseDcl), estateInfo.Contract, estate)
					// Record "land removed" update for sender estate
					estateUpdate.outLands = append(estateUpdate.outLands, assetId)
					// Record "operation" which make updates
					estateUpdate.operations = append(estateUpdate.operations, logInfo.TransactionLog.ID)
					// Safe initialize updates for moved land (assetId)
					landUpdate := dclSafeGetEstateAssetUpdate(&updates, string(metaverses.MetaverseDcl), landInfo.Contract, assetId)
					// Record "new owner" update for moved land (assetId)
					landUpdate.newOwner = receiver
					// Record "operation" which make updates
					landUpdate.operations = append(landUpdate.operations, logInfo.TransactionLog.ID)
					// continue loop
					continue
				}
			}
		}
		// Safe initialize updates for moved land (assetId)
		assetUpdate_ := dclSafeGetEstateAssetUpdate(&updates, string(metaverses.MetaverseDcl), contract, assetId)
		// Record "new owner" update for moved land (assetId)
		assetUpdate_.newOwner = receiver
		// Record "operation" which make updates
		assetUpdate_.operations = append(assetUpdate_.operations, logInfo.TransactionLog.ID)
	}

	// return updates
	return
}

//func dclGetEstateMinDistances(allDistances []*tiles_distances.MapTileMacroDistance, macroType string) []*tiles_distances.MapTileMacroDistance {
//	results := make([]*tiles_distances.MapTileMacroDistance, 0)
//	if allDistances != nil && len(allDistances) > 0 {
//		macroSubtypes := helpers.ArrayMap(allDistances, func(t *tiles_distances.MapTileMacroDistance) (bool, string) {
//			return true, t.MacroSubtype
//		}, true, "")
//		if len(macroSubtypes) > 0 {
//			for _, macroSubtype := range macroSubtypes {
//				mtDistances := helpers.ArrayFilter(allDistances, func(distance *tiles_distances.MapTileMacroDistance) bool {
//					return distance.MacroType == macroType && distance.MacroSubtype == macroSubtype
//				})
//				if len(mtDistances) > 0 {
//					minDistance := math.MaxInt
//					result := new(tiles_distances.MapTileMacroDistance)
//					found := false
//					for _, distance := range mtDistances {
//						if distance.ManDistance < minDistance {
//							found = true
//							result = distance
//							minDistance = distance.ManDistance
//						}
//					}
//					if found {
//						results = append(results, result)
//					}
//				}
//			}
//		}
//	}
//	return results
//}

func dclConvertAssetUpdateToMetadataUpdates(updates *assetUpdate, allAssets []*metaverses.MetaverseAsset, mtvInfo *metaverses.MetaverseInfo, dbInstance *mongo.Database) ([]*assetUpdateFormatted, error) {

	estateInfo := mtvInfo.GetAsset("estate")
	landInfo := mtvInfo.GetAsset("land")

	// instantiate metadata return list
	assetsUpdatesFList := make([]*assetUpdateFormatted, 0)

	// get estate asset related to updates in allAssets list
	asset := safeGetAssetForParser(updates.metaverse, updates.contract, updates.identifier, allAssets)
	if asset == nil {
		return nil, errors.New(fmt.Sprintf("{dclSaveUpdatesItemAsMetadata} estate asset not found {{%s - %s}}", updates.contract, updates.identifier))
	}

	// asset related to updates in an estate
	if updates.contract == estateInfo.Contract {
		// build metadata for `in lands` and `out lands`
		if (updates.outLands != nil && len(updates.outLands) > 0) || (updates.inLands != nil && len(updates.inLands) > 0) {
			// 1. Update Metadata Lands
			// get estate lands metadata
			aeLandsMtd, err := getUpdatableAttrOfAsset(asset, metaverses.MtvAssetAttrNameLands, dbInstance)
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
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.metaverse, landInfo.Contract, updates.outLands, dbInstance)
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
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.metaverse, landInfo.Contract, updates.inLands, dbInstance)
				if err != nil {
					return nil, err
				}
				// add lands to estate lands metadata value
				newLands = slices.Concat(newLands, coords)
			}
			// update metadata lands content
			newLandsChange := &AssetChange{
				AttrName:       metaverses.MtvAssetAttrNameLands,
				AttrDisName:    metaverses.MtvAssetAttrDisNameLands,
				DataType:       metaverses.MtvAssetAttrDataTypeStringArray,
				DataTypeParams: map[string]any{"separator": "|"},
				Value:          strings.Join(newLands, "|"),
			}
			newLandsFUpdate := &assetUpdateFormatted{
				metaverse:   asset.Metaverse,
				contract:    asset.Contract,
				identifier:  asset.AssetId,
				assetChange: newLandsChange,
			}
			assetsUpdatesFList = append(assetsUpdatesFList, newLandsFUpdate)
			// update metadata size
			newSizeChange := &AssetChange{
				AttrName:    metaverses.MtvAssetAttrNameSize,
				AttrDisName: metaverses.MtvAssetAttrDisNameSize,
				DataType:    metaverses.MtvAssetAttrDataTypeInteger,
				Value:       strconv.FormatInt(int64(len(newLands)), 10),
			}
			newSizeFUpdate := &assetUpdateFormatted{
				metaverse:   asset.Metaverse,
				contract:    asset.Contract,
				identifier:  asset.AssetId,
				assetChange: newSizeChange,
			}
			assetsUpdatesFList = append(assetsUpdatesFList, newSizeFUpdate)

			// 2. Update Metadata Distances
			// Get all distances for new lands
			//filteredDistances := dclGetDistanceByLandsCoords(newLands, focalZones)
			//// focused distance macro types
			//macroTypes := []string{"district", "plaza", "road"}
			//// Get minimal distance for all macro types
			//for _, macroType := range macroTypes {
			//
			//}

			// 2. Update Metadata Distances
			// Get all distances for new lands by focal zones
			//newLands = helpers.ArrayFilter(newLands, func(s string) bool {
			//	return strings.TrimSpace(s) != ""
			//})
			//distances := dclGetDistanceByLandsCoords(newLands, focalZones)
			//for _, distance := range distances {
			//	if distance != nil {
			//		newDistanceMtd := &AssetMetadata{
			//			Collection:    asset.Collection,
			//			AssetRef:      asset.ID,
			//			AssetContract: asset.Contract,
			//			AssetId:       asset.AssetId,
			//			Category:      MetadataTypeDistance,
			//			Name:          DistanceMetadataName(distance),
			//			DisplayName:   DistanceMetadataDisplayName(distance),
			//			DataType:      MetadataDataTypeInteger,
			//			Value:         strconv.FormatInt(int64(distance.ManDis), 10),
			//			MacroType:     distance.FocalZone.Type,
			//			MacroSubtype:  distance.FocalZone.Subtype,
			//			Date:          blockTimestamp,
			//			OperationsRef: updates.operations,
			//		}
			//		newDistanceMtd.CreatedAt = time.Now()
			//		newDistanceMtd.UpdatedAt = time.Now()
			//		metadataList = append(metadataList, newDistanceMtd)
			//	}
			//}

		}
	}

	// 2. Update Metadata Owner
	if updates.newOwner != "" {
		newOwnerChange := &AssetChange{
			AttrName:    metaverses.MtvAssetAttrNameOwner,
			AttrDisName: metaverses.MtvAssetAttrDisNameOwner,
			DataType:    metaverses.MtvAssetAttrDataTypeAddress,
			Value:       updates.newOwner,
		}
		newOwnerFUpdate := &assetUpdateFormatted{
			metaverse:   asset.Metaverse,
			contract:    asset.Contract,
			identifier:  asset.AssetId,
			assetChange: newOwnerChange,
		}
		assetsUpdatesFList = append(assetsUpdatesFList, newOwnerFUpdate)
	}

	return assetsUpdatesFList, nil
}
