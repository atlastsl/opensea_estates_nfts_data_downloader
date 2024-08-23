package operations

import (
	"decentraland_data_downloader/modules/core/assets"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
	"os"
	"slices"
	"strconv"
	"strings"
	"time"
)

func dclGetDistanceByLandsCoords(coords []string, allDistances []*tiles_distances.MapTileMacroDistance) []*tiles_distances.MapTileMacroDistance {
	filteredDistances := make([]*tiles_distances.MapTileMacroDistance, 0)
	if allDistances != nil && len(allDistances) > 0 {
		filteredDistances = helpers.ArrayFilter(allDistances, func(distance *tiles_distances.MapTileMacroDistance) bool {
			for _, coordsItem := range coords {
				if strings.HasSuffix(distance.TileSlug, "|"+coordsItem) {
					return true
				}
			}
			return false
		})
	}
	return filteredDistances
}

func dclSafeGetEstateAssetUpdate(updates *[]*assetUpdate, collection, contract, identifier string) *assetUpdate {
	i := slices.IndexFunc(*updates, func(item *assetUpdate) bool {
		return item.collection == collection && item.contract == contract && item.identifier == identifier
	})
	if i >= 0 {
		return (*updates)[i]
	} else {
		newUpdate := &assetUpdate{collection: collection, contract: contract, identifier: identifier, newOwner: "", outLands: []string{}, inLands: []string{}, operations: []primitive.ObjectID{}}
		*updates = append(*updates, newUpdate)
		return newUpdate
	}
}

func dclConvertTxLogsToAssetUpdates(txLogsInfos []*TransactionLogInfo, cltInfo *collections.CollectionInfo) (updates []*assetUpdate) {
	landInfo := cltInfo.GetAsset("land")
	estateInfo := cltInfo.GetAsset("estate")
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
					estateUpdate := dclSafeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estateInfo.Contract, estate)
					// Record "new land added" update for receiver estate
					estateUpdate.inLands = append(estateUpdate.inLands, assetId)
					// Record "operation" which make updates
					estateUpdate.operations = append(estateUpdate.operations, logInfo.TransactionLog.ID)
					// Safe initialize updates for moved land (assetId)
					landUpdate := dclSafeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landInfo.Contract, assetId)
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
					estateUpdate := dclSafeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), estateInfo.Contract, estate)
					// Record "land removed" update for sender estate
					estateUpdate.outLands = append(estateUpdate.outLands, assetId)
					// Record "operation" which make updates
					estateUpdate.operations = append(estateUpdate.operations, logInfo.TransactionLog.ID)
					// Safe initialize updates for moved land (assetId)
					landUpdate := dclSafeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), landInfo.Contract, assetId)
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
		assetUpdate_ := dclSafeGetEstateAssetUpdate(&updates, string(collections.CollectionDcl), contract, assetId)
		// Record "new owner" update for moved land (assetId)
		assetUpdate_.newOwner = receiver
		// Record "operation" which make updates
		assetUpdate_.operations = append(assetUpdate_.operations, logInfo.TransactionLog.ID)
	}

	// return updates
	return
}

func dclGetEstateMinDistance(allDistances []*tiles_distances.MapTileMacroDistance, macroType string) *tiles_distances.MapTileMacroDistance {
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

func dclConvertAssetUpdateToMetadataUpdates(updates *assetUpdate, allAssets []*Asset, blockTimestamp time.Time, cltInfo *collections.CollectionInfo, allDistances []*tiles_distances.MapTileMacroDistance, dbInstance *mongo.Database) ([]*AssetMetadata, error) {

	estateInfo := cltInfo.GetAsset("estate")
	landInfo := cltInfo.GetAsset("land")

	// instantiate metadata return list
	metadataList := make([]*AssetMetadata, 0)

	// get estate asset related to updates in allAssets list
	asset := safeGetAssetForParser(updates.collection, updates.contract, updates.identifier, allAssets)
	if asset == nil {
		return nil, errors.New("{dclSaveUpdatesItemAsMetadata} estate asset not found")
	}

	// asset related to updates in an estate
	if updates.contract == estateInfo.Contract {
		// build metadata for `in lands` and `out lands`
		if (updates.outLands != nil && len(updates.outLands) > 0) || (updates.inLands != nil && len(updates.inLands) > 0) {
			// 1. Update Metadata Lands
			// get estate lands metadata
			aeLandsMtd, err := getMetadataByEstateAsset(asset, assets.MetadataTypeLands, dbInstance)
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
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.collection, landInfo.Contract, updates.outLands, dbInstance)
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
				coords, err := getCoordinatesOfLandsByIdentifiers(updates.collection, landInfo.Contract, updates.inLands, dbInstance)
				if err != nil {
					return nil, err
				}
				// add lands to estate lands metadata value
				newLands = slices.Concat(newLands, coords)
			}
			// update metadata lands content
			newLandsMtd := &AssetMetadata{
				Collection:     asset.Collection,
				AssetRef:       asset.ID,
				AssetContract:  asset.Contract,
				AssetId:        asset.AssetId,
				Category:       MetadataTypeLands,
				Name:           MetadataNameLands,
				DisplayName:    MetadataDisNameLands,
				DataType:       MetadataDataTypeStringArray,
				DataTypeParams: map[string]any{"separator": "|"},
				Value:          strings.Join(newLands, "|"),
				Date:           blockTimestamp,
				OperationsRef:  updates.operations,
			}
			newLandsMtd.CreatedAt = time.Now()
			newLandsMtd.UpdatedAt = time.Now()
			metadataList = append(metadataList, newLandsMtd)
			// update metadata size
			newSizeMtd := &AssetMetadata{
				Collection:    asset.Collection,
				AssetRef:      asset.ID,
				AssetContract: asset.Contract,
				AssetId:       asset.AssetId,
				Category:      MetadataTypeSize,
				Name:          MetadataNameSize,
				DisplayName:   MetadataDisNameSize,
				DataType:      MetadataDataTypeInteger,
				Value:         strconv.FormatInt(int64(len(newLands)), 10),
				Date:          blockTimestamp,
				OperationsRef: updates.operations,
			}
			newSizeMtd.CreatedAt = time.Now()
			newSizeMtd.UpdatedAt = time.Now()
			metadataList = append(metadataList, newSizeMtd)

			// 2. Update Metadata Distances
			// Get all distances for new lands
			filteredDistances := dclGetDistanceByLandsCoords(newLands, allDistances)
			// focused distance macro types
			macroTypes := []string{"district", "plaza", "road"}
			// Get minimal distance for all macro types
			for _, macroType := range macroTypes {
				distance := dclGetEstateMinDistance(filteredDistances, macroType)
				if distance != nil {
					newDistanceMtd := &AssetMetadata{
						Collection:    asset.Collection,
						AssetRef:      asset.ID,
						AssetContract: asset.Contract,
						AssetId:       asset.AssetId,
						Category:      MetadataTypeDistance,
						Name:          DistanceMetadataName(distance),
						DisplayName:   DistanceMetadataDisplayName(distance),
						DataType:      MetadataDataTypeInteger,
						Value:         strconv.FormatInt(int64(distance.ManDistance), 10),
						MacroType:     distance.MacroType,
						MacroRef:      distance.MacroRef,
						Date:          blockTimestamp,
						OperationsRef: updates.operations,
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
		newOwnerMtd := &AssetMetadata{
			Collection:    asset.Collection,
			AssetRef:      asset.ID,
			AssetContract: asset.Contract,
			AssetId:       asset.AssetId,
			Category:      MetadataTypeOwner,
			Name:          MetadataNameOwner,
			DisplayName:   MetadataDisNameOwner,
			DataType:      MetadataDataTypeAddress,
			Value:         updates.newOwner,
			Date:          blockTimestamp,
			OperationsRef: updates.operations,
		}
		newOwnerMtd.CreatedAt = time.Now()
		newOwnerMtd.UpdatedAt = time.Now()
		metadataList = append(metadataList, newOwnerMtd)
	}

	return metadataList, nil
}
