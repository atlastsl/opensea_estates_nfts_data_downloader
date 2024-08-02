package tiles_distances

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/helpers"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"slices"
	"strconv"
	"strings"
)

func _dclCalculateDistance2Tiles(tile1, tile2 string) (float64, int) {
	t1Coords, t2Coords := strings.Split(tile1, ","), strings.Split(tile2, ",")
	t1X, _ := strconv.Atoi(t1Coords[0])
	t1Y, _ := strconv.Atoi(t1Coords[1])
	t2X, _ := strconv.Atoi(t2Coords[1])
	t2Y, _ := strconv.Atoi(t2Coords[1])
	return helpers.EuclidDistance(t1X, t1Y, t2X, t2Y), helpers.ManhattanDistance(t1X, t1Y, t2X, t2Y)
}

func dclCalculateTileDistances(collection collections.Collection, addData, mainData any, dbInstance *mongo.Database) error {
	macroList := addData.([]*MapMacroAug)
	tileId := mainData.(string)

	tile, err := fetchTileFromDatabase(collection, os.Getenv("DECENTRALAND_LAND_CONTRACT"), tileId, dbInstance)
	if err != nil {
		return err
	}

	var distances = make([]*MapTileMacroDistance, len(macroList))
	for i, macroAug := range macroList {
		eucDistances := make([]float64, len(macroAug.Tiles))
		manDistances := make([]int, len(macroAug.Tiles))
		for j, tile2 := range macroAug.Tiles {
			eucD, manD := _dclCalculateDistance2Tiles(tile.Coords, tile2)
			eucDistances[j] = eucD
			manDistances[j] = manD
		}
		eucDistance := slices.Min(eucDistances)
		manDistance := slices.Min(manDistances)
		distances[i] = &MapTileMacroDistance{
			TileSlug:    tiles.GetTileSlug(tile),
			TileRef:     tile.ID,
			MacroSlug:   macroAug.Macro.Slug,
			MacroRef:    macroAug.Macro.ID,
			MacroType:   macroAug.Macro.Type,
			EucDistance: eucDistance,
			ManDistance: manDistance,
		}
	}

	err = saveTileMacroDistances(distances, dbInstance)
	return err
}
