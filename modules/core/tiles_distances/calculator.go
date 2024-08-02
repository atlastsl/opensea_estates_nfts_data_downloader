package tiles_distances

import (
	"decentraland_data_downloader/modules/core/tiles"
	"decentraland_data_downloader/modules/helpers"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
	"slices"
	"strconv"
	"strings"
	"sync"
)

func _dclCalculateDistance2Tiles(tile1, tile2 string) (float64, int) {
	t1Coords, t2Coords := strings.Split(tile1, ","), strings.Split(tile2, ",")
	t1X, _ := strconv.Atoi(t1Coords[0])
	t1Y, _ := strconv.Atoi(t1Coords[1])
	t2X, _ := strconv.Atoi(t2Coords[0])
	t2Y, _ := strconv.Atoi(t2Coords[1])
	return helpers.EuclidDistance(t1X, t1Y, t2X, t2Y), helpers.ManhattanDistance(t1X, t1Y, t2X, t2Y)
}

func dclCalculateTileDistances(addData, mainData any, dbInstance *mongo.Database, wg *sync.WaitGroup) (int, error) {
	macroTList := addData.([]*MapTMacroAug)
	tile := mainData.(*tiles.MapTile)

	var distances = make([]*MapTileMacroDistance, 0)
	for _, item := range macroTList {
		if len(item.MacrosAug) > 0 {
			eucDistance, manDistance := 0.0, math.MaxInt
			minMacro := new(tiles.MapMacro)
			for _, macroAug := range item.MacrosAug {
				eucDistances := make([]float64, len(macroAug.Tiles))
				manDistances := make([]int, len(macroAug.Tiles))
				for j, tile2 := range macroAug.Tiles {
					eucD, manD := _dclCalculateDistance2Tiles(tile.Coords, tile2)
					eucDistances[j] = eucD
					manDistances[j] = manD
				}
				tEucDistance := slices.Min(eucDistances)
				tManDistance := slices.Min(manDistances)
				if tManDistance < manDistance {
					manDistance = tManDistance
					eucDistance = tEucDistance
					minMacro = macroAug.Macro
				}
			}
			if item.MacroType != "district" || manDistance == 0 {
				distance := &MapTileMacroDistance{
					TileSlug:    tiles.GetTileSlug(tile),
					TileRef:     tile.ID,
					MacroSlug:   minMacro.Slug,
					MacroRef:    minMacro.ID,
					MacroType:   minMacro.Type,
					EucDistance: eucDistance,
					ManDistance: manDistance,
				}
				distances = append(distances, distance)
			}
		}
	}

	wg.Add(1)
	go func() {
		_ = saveTileMacroDistances(distances, dbInstance)
		wg.Done()
	}()

	return len(distances), nil
}
