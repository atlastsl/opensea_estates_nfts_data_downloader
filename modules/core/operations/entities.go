package operations

import (
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/core/transactions_infos"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"time"
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

type OperationValue struct {
	Value         float64 `bson:"value,omitempty"`
	Currency      string  `bson:"currency,omitempty"`
	CurrencyPrice float64 `bson:"currency_price,omitempty"`
	ValueUsd      float64 `bson:"value_usd,omitempty"`
}

type Operation struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string             `bson:"collection,omitempty"`
	AssetRef         primitive.ObjectID `bson:"asset,omitempty"`
	AssetContract    string             `bson:"asset_contract,omitempty"`
	AssetId          string             `bson:"asset_id,omitempty"`
	TransactionHash  string             `bson:"transaction_hash,omitempty"`
	OperationType    string             `bson:"operation_type,omitempty"`
	TransactionType  string             `bson:"transaction_type,omitempty"`
	Blockchain       string             `bson:"blockchain,omitempty"`
	BlockNumber      int64              `bson:"block_number,omitempty"`
	BlockHash        string             `bson:"block_hash,omitempty"`
	Date             time.Time          `bson:"mvt_date,omitempty"`
	Sender           string             `bson:"sender,omitempty"`
	Recipient        string             `bson:"recipient,omitempty"`
	Amount           []OperationValue   `bson:"amount"`
	Fees             []OperationValue   `bson:"fees"`
}

const (
	OperationTypeFree       = "free"
	OperationTypeSale       = "sale"
	TransactionTypeMint     = "mint"
	TransactionTypeTransfer = "transfer"
)

type AssetUrl struct {
	Name string `bson:"name,omitempty"`
	Url  string `bson:"url,omitempty"`
}

type Asset struct {
	mgm.DefaultModel `bson:",inline"`
	AssetId          string     `bson:"asset_id,omitempty" json:"identifier"`
	Collection       string     `bson:"collection,omitempty"`
	Contract         string     `bson:"contract,omitempty"`
	TokenStandard    string     `bson:"token_standard,omitempty"`
	Name             string     `bson:"name,omitempty"`
	Description      string     `bson:"description,omitempty"`
	Type             string     `bson:"type,omitempty"`
	X                int        `bson:"x,omitempty"`
	Y                int        `bson:"y,omitempty"`
	Urls             []AssetUrl `bson:"urls,omitempty"`
}

type AssetMetadata struct {
	mgm.DefaultModel `bson:",inline"`
	Collection       string               `bson:"collection,omitempty"`
	AssetRef         primitive.ObjectID   `bson:"asset,omitempty"`
	AssetContract    string               `bson:"asset_contract,omitempty"`
	AssetId          string               `bson:"asset_id,omitempty"`
	Category         string               `bson:"category,omitempty"`
	Name             string               `bson:"name,omitempty"`
	DisplayName      string               `bson:"display_name,omitempty"`
	DataType         string               `bson:"data_type,omitempty"`
	DataTypeParams   map[string]any       `bson:"data_type_params,omitempty"`
	Value            string               `bson:"value,omitempty"`
	MacroRef         primitive.ObjectID   `bson:"macro,omitempty"`
	MacroType        string               `bson:"macro_type,omitempty"`
	Date             time.Time            `bson:"date,omitempty"`
	OperationsRef    []primitive.ObjectID `bson:"operations,omitempty"`
}

const (
	MetadataTypeCoordinates = "coordinates"
	MetadataTypeSize        = "size"
	MetadataTypeDistance    = "distance"
	MetadataTypeOwner       = "owner"
	MetadataTypeLands       = "lands"
)

const (
	MetadataDataTypeInteger     = "integer"
	MetadataDataTypeFloat       = "float"
	MetadataDataTypeBool        = "bool"
	MetadataDataTypeString      = "string"
	MetadataDataTypeStringArray = "string-array"
	MetadataDataTypeAddress     = "address"
)

func DistanceMetadataName(distance *tiles_distances.MapTileMacroDistance) string {
	return fmt.Sprintf("distance-to--%s", distance.MacroSlug)
}

func DistanceMetadataDisplayName(distance *tiles_distances.MapTileMacroDistance) string {
	return fmt.Sprintf("Distance to %s", distance.MacroSlug)
}

const (
	MetadataNameSize     = "size"
	MetadataNameOwner    = "owner"
	MetadataNameLands    = "lands"
	MetadataDisNameSize  = "Size"
	MetadataDisNameOwner = "Owner"
	MetadataDisNameLands = "Parcels"
)

type TransactionFull struct {
	Transaction *transactions_infos.TransactionInfo
	Logs        []*transactions_infos.TransactionLog
}

type blockNumberInput struct {
	blockNumber int
	blockchain  string
}
