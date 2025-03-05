package tiles

import (
	"decentraland_data_downloader/modules/core/collections"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"slices"
	"strings"
)

var plazaListNames map[string]string = map[string]string{
	"1092": "North Genesis Plaza",
	"1094": "North-East Genesis Plaza",
	"1134": "North-West Genesis Plaza",
	"1096": "East Genesis Plaza",
	"1131": "West Genesis Plaza",
	"1164": "Central Genesis Plaza",
	"1112": "South-East Genesis Plaza",
	"1130": "South-West Genesis Plaza",
	"1127": "South Genesis Plaza",
	"2841": "Mini Plaza",
	"2842": "Mini Plaza",
}

func _parseDclTileInfo(collection collections.Collection, tileId string, dclTile DclMapTile, districts []DclMapDistrict) (*MapTile, *MapMacro) {
	insideType, insideSubType, insideName, insideId := "nothing", "", "", ""
	if dclTile.Type == "plaza" || dclTile.Type == "road" {
		insideType = dclTile.Type
		insideName = dclTile.Name
		insideId = dclTile.EstateId
		if dclTile.Type == "plaza" {
			tmp, ok := plazaListNames[insideId]
			if ok {
				insideSubType = tmp
			} else {
				insideSubType = "Unknown Plaza"
			}
		} else {
			insideSubType = "Road"
		}
	} else if dclTile.Type == "district" {
		idx := slices.IndexFunc(districts, func(district DclMapDistrict) bool {
			return slices.Contains(district.Parcels, tileId)
		})
		insideType = dclTile.Type
		if idx >= 0 {
			insideName = districts[idx].Name
			insideId = fmt.Sprintf("Dst-%d", idx)
			insideSubType = districts[idx].Category
		} else {
			insideType = "nothing"
			insideSubType = "Nothing"
		}
	}
	var mapMacro = MapMacro{
		DefaultModel: mgm.DefaultModel{IDField: mgm.IDField{ID: primitive.NewObjectID()}},
		Collection:   string(collection),
		Contract:     os.Getenv("DECENTRALAND_LAND_CONTRACT"),
		Type:         insideType,
		Subtype:      insideSubType,
		Slug:         fmt.Sprintf("%s-%s", strings.ReplaceAll(insideName, " ", "-"), insideId),
		Name:         insideName,
		MacroID:      insideId,
	}
	var mapTile = MapTile{
		Collection: string(collection),
		Contract:   os.Getenv("DECENTRALAND_LAND_CONTRACT"),
		Coords:     dclTile.Coords,
		Type:       insideType,
		X:          dclTile.X,
		Y:          dclTile.Y,
	}
	return &mapTile, &mapMacro
}

func parseDclTileInfo(collection collections.Collection, addData, mainData any, task string, dbInstance *mongo.Database) error {
	districtsData := addData.([]DclMapDistrict)
	tileData := mainData.(DclMapTile)
	tileId := task
	mapTile, mapMacro := _parseDclTileInfo(collection, tileId, tileData, districtsData)
	var err error
	if mapTile.Type == "nothing" {
		err = saveTileInDatabase(mapTile, dbInstance)
	} else {
		mapMacro, err = saveMacroInDatabase(mapMacro, dbInstance)
		if err == nil {
			mapTile.Inside = mapMacro.ID
			err = saveTileInDatabase(mapTile, dbInstance)
		}
	}
	return err
}
