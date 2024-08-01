package assets

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"strconv"
	"strings"
	"time"
)

const dclAssetTypeLand, dclAssetTypeEstate = "parcel", "estate"

func dclParseEstateAssetInfo(osAssetInfo *helpers.OpenseaNftAsset, assetType string) *EstateAsset {
	asset := &EstateAsset{}
	asset.ID = primitive.NewObjectID()
	asset.Identifier = *osAssetInfo.Identifier
	asset.Collection = *osAssetInfo.Collection
	asset.Contract = *osAssetInfo.Contract
	if osAssetInfo.TokenStandard != nil {
		asset.TokenStandard = *osAssetInfo.TokenStandard
	}
	asset.Name = *osAssetInfo.Name
	if osAssetInfo.Description != nil {
		asset.Description = *osAssetInfo.Description
	}
	asset.Type = assetType
	assetImages := EstateAssetImages{}
	if osAssetInfo.ImageUrl != nil {
		assetImages.ImageUrl = *osAssetInfo.ImageUrl
	}
	if osAssetInfo.DisplayImageUrl != nil {
		assetImages.DisplayImageUrl = *osAssetInfo.DisplayImageUrl
	}
	if osAssetInfo.DisplayAnimationUrl != nil {
		assetImages.DisplayAnimationUrl = *osAssetInfo.DisplayAnimationUrl
	}
	assetUrls := EstateAssetUrls{}
	if osAssetInfo.OpenseaUrl != nil {
		assetUrls.OpenSeaUrl = *osAssetInfo.OpenseaUrl
	}
	if osAssetInfo.MetadataUrl != nil {
		assetUrls.MetadataUrl = *osAssetInfo.MetadataUrl
	}
	assetUrls.CollectionUrl = fmt.Sprintf("https://decentraland.org/marketplace/contracts/%s/tokens/%s", *osAssetInfo.Contract, *osAssetInfo.Identifier)
	asset.Images = assetImages
	asset.Urls = assetUrls
	if osAssetInfo.IsDisabled != nil {
		asset.IsDisabled = *osAssetInfo.IsDisabled
	}
	if osAssetInfo.IsNSFW != nil {
		asset.IsNSFW = *osAssetInfo.IsNSFW
	}
	if osAssetInfo.UpdatedAt != nil {
		asset.UpdatedDate, _ = time.Parse(time.RFC3339Nano, *osAssetInfo.UpdatedAt)
	}
	return asset
}

func dclParseEstateAssetInfoLand(osAssetInfo *helpers.OpenseaNftAsset, dbInstance *mongo.Database) (*EstateAsset, []*EstateAssetMetadata, error) {
	if osAssetInfo.Name != nil && osAssetInfo.Identifier != nil {
		asset := dclParseEstateAssetInfo(osAssetInfo, dclAssetTypeLand)
		tab := strings.Split(*osAssetInfo.Name, " ")
		coords := tab[1]
		coordsTab := strings.Split(coords, ",")
		X, _ := strconv.Atoi(coordsTab[0])
		Y, _ := strconv.Atoi(coordsTab[1])
		tile, err := fetchTileFromDatabase(collections.CollectionDcl, asset.Contract, coords, dbInstance)
		if err != nil {
			return nil, nil, err
		}
		var assetMetadata = make([]*EstateAssetMetadata, 0)
		assetMetadata = append(assetMetadata, &EstateAssetMetadata{
			EstateAssetRef: asset.ID,
			MetadataType:   MetadataTypeCoordinates,
			DataType:       MetadataDataTypeInteger,
			Name:           MetadataNameCoordinatesX,
			DisplayName:    MetadataDisNameCoordinatesX,
			Value:          strconv.FormatInt(int64(X), 10),
		})
		assetMetadata = append(assetMetadata, &EstateAssetMetadata{
			EstateAssetRef: asset.ID,
			MetadataType:   MetadataTypeCoordinates,
			DataType:       MetadataDataTypeInteger,
			Name:           MetadataNameCoordinatesY,
			DisplayName:    MetadataDisNameCoordinatesY,
			Value:          strconv.FormatInt(int64(Y), 10),
		})
		if tile != nil {
			distances, err := fetchTileMacroDistances(tile, dbInstance)
			if err != nil {
				return nil, nil, err
			}
			for _, distance := range distances {
				assetMetadata = append(assetMetadata, &EstateAssetMetadata{
					EstateAssetRef: asset.ID,
					MetadataType:   MetadataTypeDistance,
					DataType:       MetadataDataTypeInteger,
					Name:           DistanceMetadataName(&distance),
					DisplayName:    DistanceMetadataDisplayName(&distance),
					Value:          strconv.FormatInt(int64(distance.ManDistance), 10),
					MacroType:      distance.MacroType,
					MacroRef:       distance.MacroRef,
				})
			}
		}
		return asset, assetMetadata, nil
	}
	return nil, nil, errors.New("invalid decentraland LAND asset [either Name or Identifier must be specified]")
}

func dclParseEstateAssetInfoEstate(osAssetInfo *helpers.OpenseaNftAsset, _ *mongo.Database) (*EstateAsset, []*EstateAssetMetadata, error) {
	if osAssetInfo.Identifier != nil {
		asset := dclParseEstateAssetInfo(osAssetInfo, dclAssetTypeEstate)
		return asset, nil, nil
	}
	return nil, nil, errors.New("invalid decentraland ESTATE asset [Identifier must be specified]")
}

func parseEstateAssetInfo(osAssetInfo *helpers.OpenseaNftAsset, dbInstance *mongo.Database) error {
	if osAssetInfo != nil && osAssetInfo.Collection == nil && osAssetInfo.Contract != nil {

		var asset *EstateAsset = nil
		var assetMetadata []*EstateAssetMetadata
		var err error = nil
		if *(osAssetInfo.Collection) == string(collections.CollectionDcl) {
			if *(osAssetInfo.Contract) == os.Getenv("DECENTRALAND_LAND_CONTRACT") {
				asset, assetMetadata, err = dclParseEstateAssetInfoLand(osAssetInfo, dbInstance)
			} else if *(osAssetInfo.Contract) == os.Getenv("DECENTRALAND_ESTATE_CONTRACT") {
				asset, assetMetadata, err = dclParseEstateAssetInfoEstate(osAssetInfo, dbInstance)
			}
		} else {
			err = errors.New("invalid collection name")
		}

		if err != nil {
			return err
		}
		err = saveEstateAssetInDatabase(asset, dbInstance)
		if err != nil {
			return err
		}
		err = saveEstateMetadataInDatabase(assetMetadata, dbInstance)
		if err != nil {
			return err
		}

		return nil
	}
	return errors.New("invalid estate asset info")
}
