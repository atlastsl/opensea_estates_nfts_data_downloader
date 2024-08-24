package operations

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/tiles_distances"
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"errors"
	"fmt"
	"go.mongodb.org/mongo-driver/bson/primitive"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"
)

type dclAssetInfo struct {
	Id              string `json:"id"`
	Name            string `json:"name"`
	Description     string `json:"description"`
	Image           string `json:"image"`
	ExternalUrl     string `json:"external_url"`
	BackgroundColor string `json:"background_color"`
}

func dclParseCoordinatesLand(_dclAssetInfo *dclAssetInfo) (int, int, error) {
	X, Y := 0, 0
	var err error
	defNameReg := regexp.MustCompile(`^Parcel\s(-?\d+),(-?\d+)$`)
	imgUrlReg := regexp.MustCompile(`^https://api\.decentraland\.org/v(-?\d+)/parcels/(-?\d+)/(-?\d+)/map\.png`)
	matches := defNameReg.FindStringSubmatch(_dclAssetInfo.Name)
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
		matches = imgUrlReg.FindStringSubmatch(_dclAssetInfo.Image)
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
			return X, Y, errors.New("invalid estate info [cannot parse coordinates X,Y]")
		}
	}
}

func dclGetDistanceByLandCoords(coords string, allDistances []*tiles_distances.MapTileMacroDistance) []*tiles_distances.MapTileMacroDistance {
	filteredDistances := make([]*tiles_distances.MapTileMacroDistance, 0)
	if allDistances != nil && len(allDistances) > 0 {
		filteredDistances = helpers.ArrayFilter(allDistances, func(distance *tiles_distances.MapTileMacroDistance) bool {
			return strings.HasSuffix(distance.TileSlug, "|"+coords)
		})
	}
	return filteredDistances
}

func dclProcessNewLandMetadata(asset *Asset, allDistances []*tiles_distances.MapTileMacroDistance) ([]*AssetMetadata, error) {
	var assetMetadata = make([]*AssetMetadata, 0)
	if asset != nil && asset.AssetId != "" {
		coords := fmt.Sprintf("%d,%d", asset.X, asset.Y)
		distances := dclGetDistanceByLandCoords(coords, allDistances)
		if len(distances) == 0 {
			return nil, errors.New("invalid decentraland LAND asset [distances not found]")
		}
		for _, distance := range distances {
			metadata := &AssetMetadata{
				Collection:    asset.Collection,
				AssetRef:      asset.ID,
				AssetContract: asset.Contract,
				AssetId:       asset.AssetId,
				Category:      MetadataTypeDistance,
				Name:          DistanceMetadataName(distance),
				DisplayName:   DistanceMetadataDisplayName(distance),
				DataType:      MetadataDataTypeInteger,
				Value:         strconv.FormatInt(int64(distance.ManDistance), 10),
				MacroType:     distance.MacroType,
				MacroRef:      distance.MacroRef,
			}
			metadata.CreatedAt = time.Now()
			metadata.UpdatedAt = time.Now()
			assetMetadata = append(assetMetadata, metadata)
		}
		return assetMetadata, nil
	}
	return nil, errors.New("invalid decentraland LAND asset [either Name or Identifier must be specified]")
}

func dclFetchAssetInfo(cltInfo *collections.CollectionInfo, contractAddress string, assetId string, allDistances []*tiles_distances.MapTileMacroDistance) (*Asset, []*AssetMetadata, error) {
	landInfo := cltInfo.GetAsset("land")
	estateInfo := cltInfo.GetAsset("estate")
	url := ""
	assetType := ""
	if contractAddress == landInfo.Contract {
		url = strings.ReplaceAll(os.Getenv("DECENTRALAND_PARCEL_INFO_API"), "@contract", contractAddress)
		url = strings.ReplaceAll(url, "@asset", assetId)
		assetType = landInfo.Name
	} else if contractAddress == estateInfo.Contract {
		url = strings.ReplaceAll(os.Getenv("DECENTRALAND_ESTATE_INFO_API"), "@asset", assetId)
		assetType = estateInfo.Name
	}
	if url != "" {
		data, err := helpers.FetchData(url, "")
		if err != nil {
			return nil, nil, err
		}
		str, _ := json.Marshal(data)
		_dclAssetInfo := &dclAssetInfo{}
		err = json.Unmarshal(str, _dclAssetInfo)
		if err != nil {
			return nil, nil, err
		}
		if _dclAssetInfo.Id == "" {
			return nil, nil, errors.New("invalid asset id")
		}
		asset := &Asset{}
		asset.ID = primitive.NewObjectID()
		asset.CreatedAt = time.Now()
		asset.UpdatedAt = time.Now()
		asset.Collection = cltInfo.Name
		asset.Contract = contractAddress
		asset.AssetId = assetId
		asset.TokenStandard = "erc721"
		asset.Name = _dclAssetInfo.Name
		asset.Description = _dclAssetInfo.Description
		asset.Type = assetType
		assetUrls := make([]AssetUrl, 0)
		if _dclAssetInfo.ExternalUrl != "" {
			assetUrls = append(assetUrls, AssetUrl{Url: _dclAssetInfo.ExternalUrl, Name: "External URL"})
		}
		if _dclAssetInfo.Image != "" {
			assetUrls = append(assetUrls, AssetUrl{Url: _dclAssetInfo.Image, Name: "Image URL"})
		}
		asset.Urls = assetUrls
		var assetMetadata = make([]*AssetMetadata, 0)
		if contractAddress == landInfo.Contract {
			X, Y, err := dclParseCoordinatesLand(_dclAssetInfo)
			if err != nil {
				return nil, nil, err
			}
			asset.X = X
			asset.Y = Y
			assetMetadata, err = dclProcessNewLandMetadata(asset, allDistances)
			if err != nil {
				return nil, nil, err
			}
		}
		return asset, assetMetadata, nil
	} else {
		return nil, nil, errors.New("invalid estate info")
	}
}

func dclGetAssetIdentifierFromLogs(cltInfo *collections.CollectionInfo, logsInfo []*TransactionLogInfo) []map[string]string {
	result := make([]map[string]string, 0)
	inserted := make(map[string]bool)

	estateInfo := cltInfo.GetAsset("estate")
	landInfo := cltInfo.GetAsset("land")

	for _, logInfo := range logsInfo {
		contract := logInfo.TransactionLog.Address
		assetId := ""
		if logInfo.Asset != "" {
			assetId = logInfo.Asset
		} else if landInfo != nil && landInfo.Contract == logInfo.TransactionLog.Address && logInfo.Land != "" {
			assetId = logInfo.Land
		} else if estateInfo != nil && estateInfo.Contract == logInfo.TransactionLog.Address && logInfo.Estate != "" {
			assetId = logInfo.Estate
		}
		if assetId != "" {
			key := fmt.Sprintf("%s_%s", contract, assetId)
			_, ok := inserted[key]
			if !ok {
				result = append(result, map[string]string{"contract": contract, "asset_id": assetId})
				inserted[key] = true
			}
		}
	}
	return result
}
