package tiles

import (
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
)

type MapMacro struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string `bson:"collection,omitempty"`
	Contract         string `bson:"contract,omitempty"`
	Type             string `bson:"type,omitempty"`
	Subtype          string `bson:"subtype,omitempty"`
	Slug             string `bson:"slug,omitempty"`
	Name             string `bson:"name,omitempty"`
	MacroID          string `bson:"macro_id,omitempty"`
}

type MapTile struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string             `bson:"collection,omitempty"`
	Contract         string             `bson:"contract,omitempty"`
	Coords           string             `bson:"coords,omitempty"`
	X                int8               `bson:"x,omitempty"`
	Y                int8               `bson:"y,omitempty"`
	Type             string             `bson:"type,omitempty"`
	Inside           primitive.ObjectID `bson:"inside,omitempty"`
}

type DclMapTile struct {
	Coords   string `mapstructure:"id"`
	X        int8   `mapstructure:"x"`
	Y        int8   `mapstructure:"y"`
	Type     string `mapstructure:"type"`
	Name     string `mapstructure:"name"`
	EstateId string `mapstructure:"estateId"`
}

type DclMapTilesRes struct {
	Ok   bool                  `mapstructure:"ok"`
	Data map[string]DclMapTile `mapstructure:"data"`
}

type DclMapDistrict struct {
	Id           string   `mapstructure:"id"`
	Name         string   `mapstructure:"name"`
	Description  string   `mapstructure:"description"`
	Parcels      []string `mapstructure:"parcels"`
	TotalParcels uint     `mapstructure:"totalParcels"`
	Category     string   `mapstructure:"category"`
}

type DclMapDistrictRes struct {
	Ok   bool             `mapstructure:"ok"`
	Data []DclMapDistrict `mapstructure:"data"`
}
