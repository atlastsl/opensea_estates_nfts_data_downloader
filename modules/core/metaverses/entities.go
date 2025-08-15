package metaverses

import (
	"github.com/kamva/mgm/v3"
	"slices"
	"time"
)

type MetaverseAssetType string

const (
	MtvAssetTypeRealEstate MetaverseAssetType = "real-estate"
	MtvAssetTypeGood       MetaverseAssetType = "good"
	MtvAssetTypeService    MetaverseAssetType = "service"
	MtvAssetTypeOther      MetaverseAssetType = "other"
)

type MetaverseAssetSubtype string

const (
	MtvAssetStypeRELand   = "land"
	MtvAssetStypeREEstate = "estate"
	MtvAssetStypeSrSpace  = "space"
)

const (
	MtvAssetAttrNameSize     = "size"
	MtvAssetAttrNameOwner    = "owner"
	MtvAssetAttrNameLands    = "lands"
	MtvAssetAttrDisNameSize  = "Size"
	MtvAssetAttrDisNameOwner = "Owner"
	MtvAssetAttrDisNameLands = "Parcels"
)

const (
	MtvAssetAttrDataTypeInteger     = "integer"
	MtvAssetAttrDataTypeFloat       = "float"
	MtvAssetAttrDataTypeBool        = "bool"
	MtvAssetAttrDataTypeString      = "string"
	MtvAssetAttrDataTypeStringArray = "string-array"
	MtvAssetAttrDataTypeAddress     = "address"
)

type MetaverseInfoAssetAttr struct {
	AttrName       string         `bson:"attr_type,omitempty"`
	AttrDisName    string         `bson:"attr_dis_name,omitempty"`
	Constant       bool           `bson:"constant,omitempty"`
	FixedValue     string         `bson:"fixed_value,omitempty"`
	DataType       string         `bson:"data_type,omitempty"`
	DataTypeParams map[string]any `bson:"data_type_params,omitempty"`
}

type MetaverseInfoAsset struct {
	Blockchain   string                   `bson:"blockchain,omitempty"`
	Name         string                   `bson:"name,omitempty"`
	Contract     string                   `bson:"contract,omitempty"`
	AssetType    MetaverseAssetType       `bson:"asset_type,omitempty"`
	AssetSubtype MetaverseAssetSubtype    `bson:"asset_subtype,omitempty"`
	Attrs        []MetaverseInfoAssetAttr `bson:"attrs,omitempty"`
}

type MetaverseInfoLogTopic struct {
	Blockchain string   `bson:"blockchain,omitempty"`
	Name       string   `bson:"name,omitempty"`
	Hash       string   `bson:"hash,omitempty"`
	Contracts  []string `bson:"contracts,omitempty"`
	StartBlock uint64   `bson:"start_block,omitempty"`
	EndBlock   uint64   `bson:"end_block,omitempty"`
}

type MetaverseInfo struct {
	mgm.DefaultModel `bson:",inline"`
	Name             string                  `bson:"name,omitempty"`
	Blockchain       []string                `bson:"blockchain,omitempty"`
	Currency         string                  `bson:"currency,omitempty"`
	Assets           []MetaverseInfoAsset    `bson:"assets,omitempty"`
	LogTopics        []MetaverseInfoLogTopic `bson:"log_topics,omitempty"`
}

func (mtvInfo *MetaverseInfo) GetAsset(name string) *MetaverseInfoAsset {
	if mtvInfo == nil {
		return nil
	}
	for _, asset := range mtvInfo.Assets {
		if asset.Name == name {
			return &asset
		}
	}
	return nil
}

func (mtvInfo *MetaverseInfo) HasAsset(address, blockchain string) bool {
	if mtvInfo == nil {
		return false
	}
	for _, asset := range mtvInfo.Assets {
		if asset.Blockchain == blockchain && asset.Contract == address {
			return true
		}
	}
	return false
}

func (mtvInfo *MetaverseInfo) GetLogTopic(address, blockchain string, eventHex string) *MetaverseInfoLogTopic {
	if mtvInfo == nil {
		return nil
	}
	for _, logTopic := range mtvInfo.LogTopics {
		if logTopic.Blockchain == blockchain && slices.Contains(logTopic.Contracts, address) && logTopic.Hash == eventHex {
			return &logTopic
		}
	}
	return nil
}

type MetaverseAsset struct {
	mgm.DefaultModel `bson:",inline"`
	Metaverse        string             `bson:"metaverse,omitempty"`
	Blockchain       string             `bson:"blockchain,omitempty"`
	Contract         string             `bson:"contract,omitempty"`
	TokenStandard    string             `bson:"token_standard,omitempty"`
	AssetId          string             `bson:"asset_id,omitempty"`
	AssetType        MetaverseAssetType `bson:"asset_type,omitempty"`
	AssetSubtype     string             `bson:"asset_subtype,omitempty"`
	Name             string             `bson:"name,omitempty"`
	Description      string             `bson:"description,omitempty"`
	Location         string             `bson:"location,omitempty"`
	Size             float64            `bson:"size,omitempty"`
	Details          map[string]any     `bson:"details,omitempty"`
}

func (m MetaverseAsset) CollectionName() string {
	return "metaverse_assets"
}

type Currency struct {
	mgm.DefaultModel `bson:",inline"`
	Blockchain       string `bson:"blockchain,omitempty"`
	Contract         string `bson:"contract,omitempty"`
	Decimals         int64  `bson:"decimals,omitempty"`
	Name             string `bson:"name,omitempty"`
	Symbols          string `bson:"symbols,omitempty"`
	PriceMap         string `bson:"price_map,omitempty"`
	PriceSlug        string `bson:"price_slug,omitempty"`
	MainCurrency     bool   `bson:"main_currency"`
}

type CurrencyPrice struct {
	mgm.DefaultModel `bson:",inline"`
	Currency         string    `bson:"currency,omitempty"`
	Start            time.Time `bson:"start,omitempty"`
	End              time.Time `bson:"end,omitempty"`
	Open             float64   `bson:"open,omitempty"`
	High             float64   `bson:"high,omitempty"`
	Low              float64   `bson:"low,omitempty"`
	Close            float64   `bson:"close,omitempty"`
	Avg              float64   `bson:"avg,omitempty"`
	Volume           float64   `bson:"volume,omitempty"`
	MarketCap        float64   `bson:"market_cap,omitempty"`
}

type CurrencyHPrice struct { // Our example struct, you can use "-" to ignore a field
	StartDate string  `csv:"Start"`
	EndDate   string  `csv:"End"`
	Open      float64 `csv:"Open"`
	High      float64 `csv:"High"`
	Low       float64 `csv:"Low"`
	Close     float64 `csv:"Close"`
	Volume    float64 `csv:"Volume"`
	MarketCap float64 `csv:"Market Cap"`
}
