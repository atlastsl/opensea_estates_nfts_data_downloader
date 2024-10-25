package assets

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"os"
	"regexp"
	"strconv"
	"strings"
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
	if osAssetInfo.Name != nil {
		asset.Name = *osAssetInfo.Name
	} else {
		asset.Name = fmt.Sprintf("%s %s", strings.ToUpper(assetType), asset.Identifier)
	}
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
	imgUrlReg := regexp.MustCompile(`^https://api\.decentraland\.org/v(-?\d+)/parcels/(-?\d+)/(-?\d+)/map\.png`)
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
		if len(matches) == 4 {
			X, err = strconv.Atoi(matches[2])
			if err != nil {
				return X, Y, err
			}
			Y, err = strconv.Atoi(matches[3])
			if err != nil {
				return X, Y, err
			}
			return X, Y, err
		} else {
			//println(matches, *osAssetInfo.ImageUrl)
			return X, Y, errors.New("invalid estate info [cannot parse coordinates X,Y]")
		}
	}
}

func dclParseEstateAssetFilterDistancesLand(coords string, allDistances []*tiles_distances.MapTileMacroDistance) []*tiles_distances.MapTileMacroDistance {
	filteredDistances := make([]*tiles_distances.MapTileMacroDistance, 0)
	if allDistances != nil && len(allDistances) > 0 {
		filteredDistances = helpers.ArrayFilter(allDistances, func(distance *tiles_distances.MapTileMacroDistance) bool {
			return strings.HasSuffix(distance.TileSlug, "|"+coords)
		})
	}
	return filteredDistances
}

func dclParseEstateAssetInfoLand(osAssetInfo *helpers.OpenseaNftAsset, allDistances []*tiles_distances.MapTileMacroDistance) (*EstateAssetAll, error) {
	if osAssetInfo.Name != nil && osAssetInfo.ImageUrl != nil && osAssetInfo.Identifier != nil {
		asset := dclParseEstateAssetInfo(osAssetInfo, dclAssetTypeLand)
		X, Y, err := dclParseEstateAssetCoordinatesLand(osAssetInfo)
		if err != nil {
			panic(err)
			return nil, err
		}
		coords := fmt.Sprintf("%d,%d", X, Y)
		asset.X = X
		asset.Y = Y
		var assetMetadata = make([]*EstateAssetMetadata, 0)
		distances := dclParseEstateAssetFilterDistancesLand(coords, allDistances)
		if len(distances) == 0 {
			return nil, errors.New("invalid decentraland LAND asset [distances not found]")
		}
		for _, distance := range distances {
			metadata := &EstateAssetMetadata{
				MetadataType: MetadataTypeDistance,
				DataType:     MetadataDataTypeInteger,
				Name:         DistanceMetadataName(distance),
				DisplayName:  DistanceMetadataDisplayName(distance),
				Value:        strconv.FormatInt(int64(distance.ManDistance), 10),
				MacroType:    distance.MacroType,
				MacroRef:     distance.MacroRef,
			}
			metadata.CreatedAt = time.Now()
			metadata.UpdatedAt = time.Now()
			assetMetadata = append(assetMetadata, metadata)
		}
		return &EstateAssetAll{asset: asset, assetMetadata: assetMetadata}, nil
	}
	return nil, errors.New("invalid decentraland LAND asset [either Name or Identifier must be specified]")
}

func dclParseEstateAssetInfoEstate(osAssetInfo *helpers.OpenseaNftAsset) (*EstateAssetAll, error) {
	if osAssetInfo.Identifier != nil {
		asset := dclParseEstateAssetInfo(osAssetInfo, dclAssetTypeEstate)
		return &EstateAssetAll{asset: asset, assetMetadata: nil}, nil
	}
	return nil, errors.New("invalid decentraland ESTATE asset [Identifier must be specified]")
}

func pParseEstateAssetInfo(osAssetInfo *helpers.OpenseaNftAsset, allDistances []*tiles_distances.MapTileMacroDistance) (*EstateAssetAll, error) {
	if osAssetInfo != nil && osAssetInfo.Collection != nil && osAssetInfo.Contract != nil {

		var assetInfo *EstateAssetAll = nil
		var err error = nil
		if *(osAssetInfo.Collection) == string(collections.CollectionDcl) {
			if *(osAssetInfo.Contract) == os.Getenv("DECENTRALAND_LAND_CONTRACT") {
				assetInfo, err = dclParseEstateAssetInfoLand(osAssetInfo, allDistances)
			} else if *(osAssetInfo.Contract) == os.Getenv("DECENTRALAND_ESTATE_CONTRACT") {
				assetInfo, err = dclParseEstateAssetInfoEstate(osAssetInfo)
			}
		} else {
			err = errors.New("invalid collection name")
		}

		return assetInfo, err

	}
	return nil, errors.New("invalid estate asset info")
}

func parseEstateAssetInfo(osAssetInfoList []*helpers.OpenseaNftAsset, allDistances []*tiles_distances.MapTileMacroDistance, wg *sync.WaitGroup) error {
	if osAssetInfoList != nil && len(osAssetInfoList) > 0 {

		assetsInfos := make([]*EstateAssetAll, 0)
		for _, osAssetInfo := range osAssetInfoList {
			assetInfo, err := pParseEstateAssetInfo(osAssetInfo, allDistances)
			if err != nil {
				return err
			}
			assetsInfos = append(assetsInfos, assetInfo)
		}

		wg.Add(1)
		go func() {
			_ = saveEstateAssetInfoInDatabase(assetsInfos)
			wg.Done()
		}()

	}
	return nil
}
