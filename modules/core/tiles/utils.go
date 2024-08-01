package tiles

import "fmt"

func GetTileSlug(tile *MapTile) string {
	return fmt.Sprintf("%s-%s-%s", tile.Collection, tile.Contract, tile.Coords)
}
