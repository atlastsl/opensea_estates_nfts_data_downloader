package metaverses

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"errors"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"strconv"
	"strings"
	"time"
)

type DclDistrict struct {
	Id           string   `mapstructure:"id"`
	Name         string   `mapstructure:"name"`
	Description  string   `mapstructure:"description"`
	Parcels      []string `mapstructure:"parcels"`
	TotalParcels int      `mapstructure:"totalParcels"`
	Category     string   `mapstructure:"category"`
}

type DclDistrictAPIRes struct {
	Ok   bool           `mapstructure:"ok"`
	Data []*DclDistrict `mapstructure:"data"`
}

type DclParcel struct {
	Id       string `mapstructure:"id"`
	X        int8   `mapstructure:"x"`
	Y        int8   `mapstructure:"y"`
	Type     string `mapstructure:"type"`
	Name     string `mapstructure:"name"`
	EstateId string `mapstructure:"estateId"`
	NftId    string `mapstructure:"nftId"`
	TokenId  string `mapstructure:"tokenId"`
}

type DclParcelAPIRes struct {
	Ok   bool                  `mapstructure:"ok"`
	Data map[string]*DclParcel `mapstructure:"data"`
}

type DclPlaza struct {
	Id       string `mapstructure:"id"`
	EstateId string `mapstructure:"estateId"`
	Name     string `mapstructure:"name"`
}

type DclPlazaAPIRes struct {
	Ok   bool                 `mapstructure:"ok"`
	Data map[string]*DclPlaza `mapstructure:"data"`
}

type DecentralandFPParcelInfo struct {
	X       int8   `bson:"x"`
	Y       int8   `bson:"y"`
	NftId   string `bson:"nftId"`
	TokenId string `bson:"tokenId"`
}

type DclFPType string

type DclAssetREData struct {
	Description string `mapstructure:"description"`
	Id          string `mapstructure:"id"`
	Ipns        string `mapstructure:"ipns"`
	Name        string `mapstructure:"name"`
	Version     string `mapstructure:"version"`
}

type DclAssetEstate struct {
	Id        string          `mapstructure:"id"`
	Size      int             `mapstructure:"size"`
	UpdatedAt string          `mapstructure:"updatedAt"`
	Data      *DclAssetREData `mapstructure:"data"`
}

type DclAssetLand struct {
	Id        string          `mapstructure:"id"`
	TokenId   string          `mapstructure:"tokenId"`
	UpdatedAt string          `mapstructure:"updatedAt"`
	X         string          `mapstructure:"x"`
	Y         string          `mapstructure:"y"`
	Data      *DclAssetREData `mapstructure:"data"`
}

type DclAssetREResI struct {
	Estates []*DclAssetEstate `mapstructure:"estates"`
	Parcels []*DclAssetLand   `mapstructure:"parcels"`
}

type DclAssetRERes struct {
	Data *DclAssetREResI `mapstructure:"data"`
}

const (
	DclFPTypePlaza    = "plaza"
	DclFPTypeDistrict = "district"
	DclFPTypeRoad     = "road"
)

type DecentralandFocalPoint struct {
	mgm.DefaultModel `bson:",inline,omitempty"`
	FocalPointId     string                     `bson:"focal_point_id,omitempty"`
	FocalPointType   DclFPType                  `bson:"focal_point_type,omitempty"`
	EstateId         string                     `bson:"estate_id,omitempty"`
	DclId            string                     `bson:"dcl_id,omitempty"`
	Name             string                     `bson:"name,omitempty"`
	Description      string                     `bson:"description,omitempty"`
	ParcelsLoc       []string                   `bson:"parcels_loc,omitempty"`
	ParcelsCount     int                        `bson:"parcels_count,omitempty"`
	Parcels          []DecentralandFPParcelInfo `bson:"parcels,omitempty"`
	Category         string                     `bson:"category,omitempty"`
}

func getDclDistrictData() ([]*DclDistrict, error) {
	filePath := os.Getenv("DECENTRALAND_DISTRICTS_FILE")
	resDistricts := DclDistrictAPIRes{}
	err := helpers.ReadJsonFile(filePath, &resDistricts)
	if err != nil {
		return nil, err
	}
	return resDistricts.Data, nil
}

func getDclTilesData() (map[string]*DclParcel, error) {
	filePath := os.Getenv("DECENTRALAND_PARCELS_FILE")
	resTiles := DclParcelAPIRes{}
	err := helpers.ReadJsonFile(filePath, &resTiles)
	if err != nil {
		return nil, err
	}
	return resTiles.Data, nil
}

func getDclPlazaData() (map[string]*DclPlaza, error) {
	filePath := os.Getenv("DECENTRALAND_PLAZAS_FILE")
	resTiles := DclPlazaAPIRes{}
	err := helpers.ReadJsonFile(filePath, &resTiles)
	if err != nil {
		return nil, err
	}
	return resTiles.Data, nil
}

func getDclAssetData(assetType string, take, skip int) (*DclAssetREResI, error) {
	query := ""
	if assetType == "land" {
		query = `{
	parcels(orderBy: id, orderDirection: asc, first: @take, skip: @skip) {
		id
		tokenId
		x
		y
		operator
		updatedAt
		data {
			description
			name
			ipns
			id
			version
		}
	}
}`
	} else {
		query = `{
	estates(orderBy: id, orderDirection: asc, first: @take, skip: @skip) {
		id
		size
		operator
		updatedAt
		data {
			description
			name
			ipns
			id
			version
		}
	}
}`
	}
	query = strings.ReplaceAll(query, "@skip", strconv.Itoa(skip))
	query = strings.ReplaceAll(query, "@take", strconv.Itoa(take))
	//query = strings.ReplaceAll(query, "\n", "\\n")
	//query = strings.ReplaceAll(query, "\t", "\\t")
	payloadJson := map[string]any{
		"query": query,
	}
	payload, err := json.MarshalIndent(payloadJson, "", "  ")
	if err != nil {
		return nil, err
	}
	apiKey := fmt.Sprintf("Bearer %s", os.Getenv("THEGRAPH_API_KEY"))
	graphApiUrl := os.Getenv("THEGRAPH_API_URL")

	//client := graphql.NewClient(graphApiUrl)
	//request := graphql.NewRequest(query)
	//request.Header.Set("Authorization", fmt.Sprintf("Bearer %s", apiKey))
	//request.Header.Set("Cache-Control", "no-cache")

	jsonResPlaza := map[string]any{}
	err = helpers.PostData(graphApiUrl, apiKey, payload, &jsonResPlaza)
	//err := client.Run(context.Background(), request, &jsonResPlaza)
	if err == nil && jsonResPlaza != nil {
		errorRes, ok := jsonResPlaza["errors"]
		if ok {
			errorResArr := errorRes.([]any)
			errorMsgArr := helpers.ArrayMap(errorResArr, func(t any) (bool, string) {
				return true, t.(map[string]any)["message"].(string)
			}, true, "")
			err = errors.New(strings.Join(errorMsgArr, "\n"))
		}
	}
	if err != nil {
		return nil, err
	}
	resTiles := DclAssetRERes{}
	err = helpers.ConvertMapToStruct(jsonResPlaza, &resTiles)
	if err != nil {
		return nil, err
	}
	return resTiles.Data, nil
}

func parseDclDistrictData(parcels map[string]*DclParcel, districts []*DclDistrict) []*DecentralandFocalPoint {
	pDistricts := make([]*DecentralandFocalPoint, len(districts))
	for i, district := range districts {
		dParcels := make([]DecentralandFPParcelInfo, 0)
		for _, parcel := range district.Parcels {
			dclParcel, ok := parcels[parcel]
			if ok {
				dParcel := DecentralandFPParcelInfo{
					X:       dclParcel.X,
					Y:       dclParcel.Y,
					NftId:   dclParcel.NftId,
					TokenId: dclParcel.TokenId,
				}
				dParcels = append(dParcels, dParcel)
			}
		}
		fpDistrict := &DecentralandFocalPoint{
			FocalPointId:   district.Id,
			FocalPointType: DclFPTypeDistrict,
			DclId:          helpers.CodeFlattenString(district.Name),
			Name:           district.Name,
			Description:    district.Description,
			ParcelsLoc:     district.Parcels,
			ParcelsCount:   district.TotalParcels,
			Parcels:        dParcels,
			Category:       district.Category,
		}
		pDistricts[i] = fpDistrict
	}
	return pDistricts
}

func parseDclRoadData(parcels map[string]*DclParcel) []*DecentralandFocalPoint {
	fpRoadsMap := map[string]*DecentralandFocalPoint{}
	for s, parcel := range parcels {
		if parcel.Type == "road" {
			estateId := parcel.EstateId
			fpRoad, fpRoadExists := fpRoadsMap[estateId]
			if !fpRoadExists {
				fpRoad = &DecentralandFocalPoint{
					FocalPointId:   estateId,
					FocalPointType: DclFPTypeRoad,
					DclId:          estateId,
					Name:           fmt.Sprintf("Road %s", parcel.EstateId),
					Description:    parcel.Name,
					ParcelsLoc:     make([]string, 0),
					ParcelsCount:   0,
					Parcels:        make([]DecentralandFPParcelInfo, 0),
				}
				fpRoadsMap[estateId] = fpRoad
			}
			fpRoad.ParcelsLoc = append(fpRoad.ParcelsLoc, s)
			fpRoad.ParcelsCount = fpRoad.ParcelsCount + 1
			fpRoad.Parcels = append(fpRoad.Parcels, DecentralandFPParcelInfo{
				X:       parcel.X,
				Y:       parcel.Y,
				NftId:   parcel.NftId,
				TokenId: parcel.TokenId,
			})
		}
	}
	fpRoads := make([]*DecentralandFocalPoint, 0)
	for _, road := range fpRoadsMap {
		fpRoads = append(fpRoads, road)
	}
	return fpRoads
}

func parseDclPlazaData(parcels map[string]*DclParcel, plazas map[string]*DclPlaza) []*DecentralandFocalPoint {
	fpPlazas := make([]*DecentralandFocalPoint, len(plazas))
	fpPlazasRef := make(map[string]int)
	i := 0
	for _, plaza := range plazas {
		fpPlazas[i] = &DecentralandFocalPoint{
			FocalPointId:   plaza.Id,
			FocalPointType: DclFPTypePlaza,
			EstateId:       plaza.EstateId,
			DclId:          plaza.Id,
			Name:           plaza.Name,
			ParcelsLoc:     make([]string, 0),
			ParcelsCount:   0,
			Parcels:        make([]DecentralandFPParcelInfo, 0),
		}
		fpPlazasRef[plaza.EstateId] = i
		i++
	}
	for s, parcel := range parcels {
		if parcel.Type == "plaza" {
			estateId := parcel.EstateId
			fpPlazaIdx, _ := fpPlazasRef[estateId]
			fpPlaza := fpPlazas[fpPlazaIdx]
			fpPlaza.ParcelsLoc = append(fpPlaza.ParcelsLoc, s)
			fpPlaza.ParcelsCount = fpPlaza.ParcelsCount + 1
			fpPlaza.Parcels = append(fpPlaza.Parcels, DecentralandFPParcelInfo{
				X:       parcel.X,
				Y:       parcel.Y,
				NftId:   parcel.NftId,
				TokenId: parcel.TokenId,
			})
		}
	}
	return fpPlazas
}

func parseDclEstateData(estate *DclAssetEstate, dclMtvInfo *MetaverseInfo) *MetaverseAsset {
	estateName, estateDesc := "", ""
	if estate.Data != nil {
		estateName = estate.Data.Name
		estateDesc = estate.Data.Description
	}
	jsonEstateMap := map[string]any{}
	_ = helpers.ConvertStructToMap(estate, &jsonEstateMap)
	assetInfo := getMetaverseInfoAsset(dclMtvInfo, "estate", EthereumBlockchain)
	asset := &MetaverseAsset{
		Metaverse:     string(MetaverseDcl),
		Blockchain:    EthereumBlockchain,
		Contract:      assetInfo.Contract,
		TokenStandard: "erc721",
		AssetId:       estate.Id,
		AssetType:     MtvAssetTypeRealEstate,
		AssetSubtype:  MtvAssetStypeREEstate,
		Name:          estateName,
		Description:   estateDesc,
		Location:      "",
		Size:          0,
		Details:       jsonEstateMap,
	}
	if estate.UpdatedAt != "" {
		assetUpdatedAt, _ := strconv.ParseInt(estate.UpdatedAt, 10, 64)
		asset.CreatedAt = time.UnixMilli(assetUpdatedAt * 1000)
		asset.UpdatedAt = time.UnixMilli(assetUpdatedAt * 1000)
	}
	return asset
}

func parseDclLandData(land *DclAssetLand, dclMtvInfo *MetaverseInfo) *MetaverseAsset {
	landName, landDesc := "", ""
	if land.Data != nil {
		landName = land.Data.Name
		landDesc = land.Data.Description
	}
	jsonLandMap := map[string]any{}
	_ = helpers.ConvertStructToMap(land, &jsonLandMap)
	assetInfo := getMetaverseInfoAsset(dclMtvInfo, "land", EthereumBlockchain)
	asset := &MetaverseAsset{
		Metaverse:     string(MetaverseDcl),
		Blockchain:    EthereumBlockchain,
		Contract:      assetInfo.Contract,
		TokenStandard: "erc721",
		AssetId:       land.TokenId,
		AssetType:     MtvAssetTypeRealEstate,
		AssetSubtype:  MtvAssetStypeRELand,
		Name:          landName,
		Description:   landDesc,
		Location:      fmt.Sprintf("%s,%s", land.X, land.Y),
		Size:          1,
		Details:       jsonLandMap,
	}
	if land.UpdatedAt != "" {
		assetUpdatedAt, _ := strconv.ParseInt(land.UpdatedAt, 10, 64)
		asset.CreatedAt = time.UnixMilli(assetUpdatedAt * 1000)
		asset.UpdatedAt = time.UnixMilli(assetUpdatedAt * 1000)
	}
	return asset
}

func parseDclAssetData(assetDataRes *DclAssetREResI, assetType string, dclMtvInfo *MetaverseInfo) ([]*MetaverseAsset, error) {
	assets := make([]*MetaverseAsset, 0)
	if assetType == "land" {
		for _, rawLand := range assetDataRes.Parcels {
			land := parseDclLandData(rawLand, dclMtvInfo)
			assets = append(assets, land)
		}
	} else {
		for _, rawEstate := range assetDataRes.Estates {
			estate := parseDclEstateData(rawEstate, dclMtvInfo)
			assets = append(assets, estate)
		}
	}
	return assets, nil
}

func saveDclFocalPoints(focalPoints []*DecentralandFocalPoint, dbInstance *mongo.Database) error {
	if focalPoints != nil && len(focalPoints) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &DecentralandFocalPoint{})

		bdOperations := make([]mongo.WriteModel, len(focalPoints))
		for i, focalPoint := range focalPoints {
			var filterPayload = bson.M{"focal_point_id": focalPoint.FocalPointId, "focal_point_type": focalPoint.FocalPointType}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(focalPoint).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func getterDclRequestOrder() ([]string, error) {
	assetsTake, landsMax, estatesMax := 1000, 100000, 6000
	tasks := []string{"extra"}
	for i := 39000; i < landsMax; i += assetsTake {
		task := fmt.Sprintf("asset:land:%d,%d", i, i+assetsTake)
		tasks = append(tasks, task)
	}
	for i := 0; i < estatesMax; i += assetsTake {
		task := fmt.Sprintf("asset:estate:%d,%d", i, i+assetsTake)
		tasks = append(tasks, task)
	}
	return tasks, nil
}

func processDclTaskExtra(task string, params map[string]any, dbInstance *mongo.Database) error {
	districts, err := getDclDistrictData()
	if err != nil {
		return err
	}
	tiles, err := getDclTilesData()
	if err != nil {
		return err
	}
	plazas, err := getDclPlazaData()
	if err != nil {
		return err
	}
	disFocalPoints := parseDclDistrictData(tiles, districts)
	plzFocalPoints := parseDclPlazaData(tiles, plazas)
	rdsFocalPoints := parseDclRoadData(tiles)
	allFocalPoints := make([]*DecentralandFocalPoint, 0)
	allFocalPoints = append(allFocalPoints, disFocalPoints...)
	allFocalPoints = append(allFocalPoints, rdsFocalPoints...)
	allFocalPoints = append(allFocalPoints, plzFocalPoints...)
	err = saveDclFocalPoints(allFocalPoints, dbInstance)
	return err
}

func processDclTaskAsset(task string, params map[string]any, dbInstance *mongo.Database) ([]*MetaverseAsset, error) {
	metaverseInfo := params["metaverseInfo"].(*MetaverseInfo)
	command := strings.Split(task, ":")
	assetType, limits := command[1], command[2]
	limitsArr := strings.Split(limits, ",")
	limitMin, _ := strconv.Atoi(limitsArr[0])
	limitMax, _ := strconv.Atoi(limitsArr[1])
	apiRes, err := getDclAssetData(assetType, limitMax-limitMin, limitMin)
	if err != nil {
		return nil, err
	}
	assets, err := parseDclAssetData(apiRes, assetType, metaverseInfo)
	if err != nil {
		return nil, err
	}
	return assets, nil
}
