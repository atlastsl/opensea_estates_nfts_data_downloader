package assets

import (
	"decentraland_data_downloader/modules/core/tiles_distances"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"strings"
	"time"
)

type EstateAssetImages struct {
	ImageUrl            string `bson:"image_url,omitempty"`
	DisplayImageUrl     string `bson:"display_image_url,omitempty"`
	AnimationUrl        string `bson:"animation_url,omitempty"`
	DisplayAnimationUrl string `bson:"display_animation_url,omitempty"`
}

type EstateAssetUrls struct {
	MetadataUrl   string `bson:"metadata_url,omitempty"`
	OpenSeaUrl    string `bson:"opensea_url,omitempty"`
	CollectionUrl string `bson:"collection_url,omitempty"`
}

type EstateAsset struct {
	mgm.DefaultModel `bson:",inline"`
	Identifier       string            `bson:"identifier,omitempty" json:"identifier"`
	Collection       string            `bson:"collection,omitempty"`
	Contract         string            `bson:"contract,omitempty"`
	TokenStandard    string            `bson:"token_standard,omitempty"`
	Name             string            `bson:"name,omitempty"`
	Description      string            `bson:"description,omitempty"`
	Type             string            `bson:"type,omitempty"`
	Images           EstateAssetImages `bson:"images,omitempty"`
	Urls             EstateAssetUrls   `bson:"urls,omitempty"`
	IsDisabled       bool              `bson:"is_disabled"`
	IsNSFW           bool              `bson:"is_nsfw"`
	IsSuspicious     bool              `bson:"is_suspicious"`
	UpdatedDate      time.Time         `bson:"updated_date,omitempty"`
}

type EstateAssetMetadata struct {
	mgm.DefaultModel `bson:",inline"`
	EstateAssetRef   primitive.ObjectID `bson:"estate_asset,omitempty"`
	MetadataType     string             `bson:"metadata_type,omitempty"`
	DataType         string             `bson:"data_type,omitempty"`
	Name             string             `bson:"name,omitempty"`
	DisplayName      string             `bson:"display_name,omitempty"`
	Value            string             `bson:"value,omitempty"`
	MacroRef         primitive.ObjectID `bson:"macro,omitempty"`
	MacroType        string             `bson:"macro_type,omitempty"`
	UpdateDate       time.Time          `bson:"update_date,omitempty"`
	EventRef         primitive.ObjectID `bson:"event,omitempty"`
}

const (
	MetadataTypeCoordinates = "coordinates"
	MetadataTypeSize        = "size"
	MetadataTypeDistance    = "distance"
	MetadataTypeOwner       = "owner"
	MetadataTypeLands       = "lands"
)

const (
	MetadataDataTypeInteger = "integer"
	MetadataDataTypeFloat   = "float"
	MetadataDataTypeBool    = "bool"
	MetadataDataTypeString  = "string"
)

const (
	MetadataNameCoordinatesX = "X"
	MetadataNameCoordinatesY = "Y"
)

const (
	MetadataDisNameCoordinatesX = "Position X"
	MetadataDisNameCoordinatesY = "Position Y"
)

func DistanceMetadataName(distance *tiles_distances.MapTileMacroDistance) string {
	return fmt.Sprintf("distance-to--%s", distance.MacroSlug)
}

func DistanceMetadataDisplayName(distance *tiles_distances.MapTileMacroDistance) string {
	tmp := strings.Split(distance.MacroSlug, "-")
	return fmt.Sprintf("Distance to %s", tmp[2])
}

const (
	MetadataNameSize    = "size"
	MetadataDisNameSize = "Size"
)

const (
	MetadataNameOwner    = "owner"
	MetadataDisNameOwner = "Owner"
)

const (
	MetadataNameLands    = "lands"
	MetadataDisNameLands = "Parcels"
)
