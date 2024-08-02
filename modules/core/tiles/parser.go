package tiles

import (
	"decentraland_data_downloader/modules/core/collections"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"slices"
)

func _parseDclTileInfo(collection collections.Collection, tileId string, dclTile DclMapTile, districts []DclMapDistrict) (*MapTile, *MapMacro) {
	insideType, insideName, insideId := "nothing", "", ""
	if dclTile.Type == "plaza" || dclTile.Type == "road" {
		insideType = dclTile.Type
		insideName = dclTile.Name
		insideId = dclTile.EstateId
	} else if dclTile.Type == "district" {
		idx := slices.IndexFunc(districts, func(district DclMapDistrict) bool {
			return slices.Contains(district.Parcels, tileId)
		})
		insideType = dclTile.Type
		if idx >= 0 {
			insideName = districts[idx].Name
			insideId = fmt.Sprintf("District-%d", idx)
		} else {
			insideType = "nothing"
		}
	}
	var mapMacro = MapMacro{
		DefaultModel: mgm.DefaultModel{IDField: mgm.IDField{ID: primitive.NewObjectID()}},
		Collection:   string(collection),
		Contract:     os.Getenv("DECENTRALAND_LAND_CONTRACT"),
		Type:         insideType,
		Slug:         fmt.Sprintf("%s-%s", insideName, insideId),
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
