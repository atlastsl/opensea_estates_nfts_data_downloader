package tiles

import (
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MapMacro struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string `bson:"collection,omitempty"`
	Contract         string `bson:"nft_contract,omitempty"`
	Type             string `bson:"type,omitempty"`
	Slug             string `bson:"slug,omitempty"`
	Name             string `bson:"name,omitempty"`
	MacroID          string `bson:"macro_id,omitempty"`
}

type MapTile struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string             `bson:"collection,omitempty"`
	Contract         string             `bson:"nft_contract,omitempty"`
	Coords           string             `bson:"coords,omitempty"`
	X                int8               `bson:"x,omitempty"`
	Y                int8               `bson:"y,omitempty"`
	Type             string             `bson:"type,omitempty"`
	Inside           primitive.ObjectID `bson:"inside,omitempty"`
}

type DclMapTileId string

type DclMapTile struct {
	Coords   string `mapstructure:"id"`
	X        int8   `mapstructure:"x"`
	Y        int8   `mapstructure:"y"`
	Type     string `mapstructure:"type"`
	Name     string `mapstructure:"name"`
	EstateId string `mapstructure:"estateId"`
}

type DclMapTiles map[DclMapTileId]DclMapTile

type DclMapTilesRes struct {
	Ok   bool        `mapstructure:"ok"`
	Data DclMapTiles `mapstructure:"data"`
}

type DclMapDistrict struct {
	Id           string         `mapstructure:"id"`
	Name         string         `mapstructure:"name"`
	Description  string         `mapstructure:"description"`
	Parcels      []DclMapTileId `mapstructure:"parcels"`
	TotalParcels uint           `mapstructure:"totalParcels"`
}

type DclMapDistrictRes struct {
	Ok   bool             `mapstructure:"ok"`
	Data []DclMapDistrict `mapstructure:"data"`
}
