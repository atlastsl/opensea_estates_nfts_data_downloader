package operations

import (
	"decentraland_data_downloader/modules/core/transactions_infos"
	"fmt"
	"strings"
)

type TransactionLogInfo struct {
	EventName         string
	IsCollectionAsset bool
	From              string
	To                string
	Amount            string
	Asset             string
	Land              string
	Estate            string
	TransactionLog    *transactions_infos.TransactionLog
}

type TransactionFull struct {
	Transaction *transactions_infos.TransactionInfo
	Logs        []*transactions_infos.TransactionLog
}

type BlockNumberInput struct {
	BlockNumber int    `json:"block_number"`
	Blockchain  string `json:"blockchain"`
}

type MapFocalZone struct {
	Collection string   `mapstructure:"collection"`
	Contract   string   `mapstructure:"contract"`
	Type       string   `mapstructure:"type"`
	Subtype    string   `mapstructure:"subtype"`
	Slug       string   `mapstructure:"slug"`
	Name       string   `mapstructure:"name"`
	FZoneId    string   `mapstructure:"f_zone_id"`
	Parcels    []string `mapstructure:"parcels"`
}

type MapFocalZoneDistance struct {
	X         int
	Y         int
	FocalZone *MapFocalZone
	EucDis    float64
	ManDis    int
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

func DistanceMetadataName(distance *MapFocalZoneDistance) string {
	fSubType := strings.ReplaceAll(strings.ToLower(distance.FocalZone.Subtype), " ", "_")
	return fmt.Sprintf("distance-to--%s", fSubType)
}

func DistanceMetadataDisplayName(distance *MapFocalZoneDistance) string {
	return fmt.Sprintf("Distance to %s", distance.FocalZone.Subtype)
}
