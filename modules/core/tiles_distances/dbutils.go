package tiles_distances

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func getMacroFromDatabase(collection collections.Collection, contract string, dbInstance *mongo.Database) ([]*MapMacroAug, error) {
	macrosCollection := database.CollectionInstance(dbInstance, &tiles.MapMacro{})
	find, err := macrosCollection.Find(context.Background(), bson.M{"contract": contract, "collection": string(collection)})
	if err != nil {
		return nil, err
	}
	var macros []tiles.MapMacro
	err = find.All(context.Background(), &macros)
	if err != nil {
		return nil, err
	}
	err = find.Close(context.Background())
	if err != nil {
		return nil, err
	}
	tilesCollection := database.CollectionInstance(dbInstance, &tiles.MapTile{})
	find, err = tilesCollection.Find(context.Background(), bson.M{"contract": contract, "collection": string(collection), "inside": bson.M{"$exists": true, "$ne": nil}})
	if err != nil {
		return nil, err
	}
	var _tiles []tiles.MapTile
	err = find.All(context.Background(), &_tiles)
	if err != nil {
		return nil, err
	}
	err = find.Close(context.Background())
	if err != nil {
		return nil, err
	}
	var macroList = make([]*MapMacroAug, 0)
	for _, macro := range macros {
		tilesList := helpers.ArrayFilter(_tiles, func(tile tiles.MapTile) bool {
			return macro.ID.String() == tile.Inside.String()
		})
		tilesIds := helpers.ArrayMap(tilesList, func(t tiles.MapTile) (bool, string) {
			return true, t.Coords
		}, true, "")
		macroList = append(macroList, &MapMacroAug{Macro: &macro, Tiles: tilesIds})
	}
	return macroList, nil
}

func getTilesToWorkFromDatabase(collection collections.Collection, contract string, dbInstance *mongo.Database) (map[string]*tiles.MapTile, error) {
	tilesCollection := database.CollectionInstance(dbInstance, &tiles.MapTile{})
	cursor, err := tilesCollection.Find(context.Background(), bson.M{"contract": contract, "collection": string(collection)})
	if err != nil {
		return nil, err
	}
	tilesList := make([]tiles.MapTile, 0)
	err = cursor.All(context.Background(), &tilesList)
	if err != nil {
		return nil, err
	}
	tilesMap := make(map[string]*tiles.MapTile)
	for _, tile := range tilesList {
		tilesMap[tile.Coords] = &tile
	}
	return tilesMap, nil
}

func fetchTileFromDatabase(collection collections.Collection, contract, coords string, dbInstance *mongo.Database) (*tiles.MapTile, error) {
	tile := &tiles.MapTile{}
	tilesCollection := database.CollectionInstance(dbInstance, tile)
	err := tilesCollection.FirstWithCtx(context.Background(), bson.M{"contract": contract, "collection": string(collection), "coords": coords}, tile)
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		} else {
			return nil, nil
		}
	} else {
		return tile, nil
	}
}

func saveTileMacroDistances(distances []*MapTileMacroDistance, dbInstance *mongo.Database) error {
	if distances != nil && len(distances) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &MapTileMacroDistance{})
		operations := make([]mongo.WriteModel, len(distances))
		for i, distance := range distances {
			filterPayload := bson.M{"tile_slug": distance.TileSlug, "macro_slug": distance.MacroSlug}
			operations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(distance).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), operations)
		return err
	}
	return nil
}
