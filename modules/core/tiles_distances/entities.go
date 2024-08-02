package tiles_distances

import (
	"decentraland_data_downloader/modules/core/tiles"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MapTileMacroDistance struct {
	mgm.DefaultModel `bson:",inline"`
	TileSlug         string             `bson:"tile_slug,omitempty"`
	TileRef          primitive.ObjectID `bson:"tile_ref,omitempty"`
	MacroSlug        string             `bson:"macro_slug,omitempty"`
	MacroRef         primitive.ObjectID `bson:"macro_ref,omitempty"`
	MacroType        string             `bson:"macro_type,omitempty"`
	EucDistance      float64            `bson:"euc_distance"`
	ManDistance      int                `bson:"man_distance"`
}

type MapMacroAug struct {
	Macro *tiles.MapMacro
	Tiles []string
}

type MapTMacroAug struct {
	MacroType string
	MacrosAug []*MapMacroAug
}
