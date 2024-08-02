package assets

import (
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"regexp"
	"strconv"
	"sync"
	"time"
)

const dclAssetTypeLand, dclAssetTypeEstate = "parcel", "estate"

func dclParseEstateAssetInfo(osAssetInfo *helpers.OpenseaNftAsset, assetType string) *EstateAsset {
	asset := &EstateAsset{}
	asset.CreatedAt = time.Now()
	asset.UpdatedAt = time.Now()
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

func dclParseEstateAssetCoordinatesLand(osAssetInfo *helpers.OpenseaNftAsset) (int, int, error) {
	X, Y := 0, 0
	var err error
	defNameReg := regexp.MustCompile(`^Parcel\s(-?\d+),(-?\d+)$`)
	imgUrlReg := regexp.MustCompile(`^https://api\.decentraland\.org/v2/parcels/(-?\d+)/(-?\d+)/map\.png`)
	matches := defNameReg.FindStringSubmatch(*osAssetInfo.Name)
	if len(matches) == 3 {
		X, err = strconv.Atoi(matches[1])
		if err != nil {
			return X, Y, err
		}
		Y, err = strconv.Atoi(matches[2])
		if err != nil {
			return X, Y, err
		}
		return X, Y, err
	} else {
		matches = imgUrlReg.FindStringSubmatch(*osAssetInfo.ImageUrl)
		if len(matches) == 3 {
			X, err = strconv.Atoi(matches[1])
			if err != nil {
				return X, Y, err
			}
			Y, err = strconv.Atoi(matches[2])
			if err != nil {
				return X, Y, err
			}
			return X, Y, err
		} else {
			println(matches, *osAssetInfo.ImageUrl)
			return X, Y, errors.New("invalid estate info [cannot parse coordinates X,Y]")
		}
	}
}

func dclParseEstateAssetInfoLand(osAssetInfo *helpers.OpenseaNftAsset, dbInstance *mongo.Database) (*EstateAsset, []*EstateAssetMetadata, error) {
	if osAssetInfo.Name != nil && osAssetInfo.Identifier != nil {
		asset := dclParseEstateAssetInfo(osAssetInfo, dclAssetTypeLand)
		X, Y, err := dclParseEstateAssetCoordinatesLand(osAssetInfo)
		if err != nil {
			panic(err)
			return nil, nil, err
		}
		coords := fmt.Sprintf("%d,%d", X, Y)
		asset.X = X
		asset.Y = Y
		tile, err := fetchTileFromDatabase(collections.CollectionDcl, asset.Contract, coords, dbInstance)
		if err != nil {
			return nil, nil, err
		}
		var assetMetadata = make([]*EstateAssetMetadata, 0)
		if tile != nil {
			distances, err := fetchTileMacroDistances(tile, dbInstance)
			if err != nil {
				return nil, nil, err
			}
			for _, distance := range distances {
				metadata := &EstateAssetMetadata{
					MetadataType: MetadataTypeDistance,
					DataType:     MetadataDataTypeInteger,
					Name:         DistanceMetadataName(&distance),
					DisplayName:  DistanceMetadataDisplayName(&distance),
					Value:        strconv.FormatInt(int64(distance.ManDistance), 10),
					MacroType:    distance.MacroType,
					MacroRef:     distance.MacroRef,
				}
				metadata.CreatedAt = time.Now()
				metadata.UpdatedAt = time.Now()
				assetMetadata = append(assetMetadata, metadata)
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

func saveEstateAssetInfoInDatabase(asset *EstateAsset, assetMetadata []*EstateAssetMetadata) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)
	assetId, err := saveEstateAssetInDatabase(asset, dbInstance)
	if err != nil {
		return err
	}
	err = saveEstateMetadataInDatabase(assetMetadata, assetId, dbInstance)
	if err != nil {
		return err
	}
	return nil
}

func parseEstateAssetInfo(osAssetInfo *helpers.OpenseaNftAsset, dbInstance *mongo.Database, wg *sync.WaitGroup) error {
	if osAssetInfo != nil && osAssetInfo.Collection != nil && osAssetInfo.Contract != nil {

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

		wg.Add(1)
		go func() {
			_ = saveEstateAssetInfoInDatabase(asset, assetMetadata)
			wg.Done()
		}()

		return nil
	}
	return errors.New("invalid estate asset info")
}
