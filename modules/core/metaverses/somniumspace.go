package metaverses

import (
	"decentraland_data_downloader/modules/helpers"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"os"
	"strconv"
	"strings"
	"time"
)

type ISmnLandLocation struct {
	X float64 `mapstructure:"x"`
	Y float64 `mapstructure:"z"`
	Z float64 `mapstructure:"y"`
}

type ISmdLand struct {
	ActivationDate string            `mapstructure:"activationDate"`
	AddedHeight    float64           `mapstructure:"addedHeight"`
	Area           int               `mapstructure:"area"`
	Bid            bool              `mapstructure:"bid"`
	Construction   int               `mapstructure:"construction"`
	Height         int               `mapstructure:"height"`
	Id             int               `mapstructure:"id"`
	IsAvailable    bool              `mapstructure:"isAvailable"`
	IsNew          bool              `mapstructure:"isNew"`
	IsSafe         bool              `mapstructure:"isSafe"`
	Location       *ISmnLandLocation `mapstructure:"location"`
	Name           string            `mapstructure:"name"`
	OwnerId        string            `mapstructure:"ownerId"`
	OwnerName      string            `mapstructure:"ownerName"`
	RentStatus     int               `mapstructure:"rentStatus"`
	RenterId       string            `mapstructure:"renterId"`
	Road           bool              `mapstructure:"road"`
	Size           string            `mapstructure:"size"`
	Slo            bool              `mapstructure:"slo"`
	SnapshotUrl    string            `mapstructure:"snapshotUrl"`
	UnityId        int               `mapstructure:"unityId"`
	Waterfront     bool              `mapstructure:"waterfront"`
}

type ISmnLandItemCoordinate struct {
	X float64 `mapstructure:"x"`
	Y float64 `mapstructure:"y"`
}

type ISmnLandItemLastSale struct {
	DaysAgo          float64 `mapstructure:"days_ago"`
	LastSaleDate     string  `mapstructure:"last_sale_date"`
	LastSalePrice    float64 `mapstructure:"last_sale_price"`
	LastSalePriceEth float64 `mapstructure:"last_sale_price_eth"`
	LastSalePriceUsd float64 `mapstructure:"last_sale_price_usd"`
	Name             string  `mapstructure:"name"`
	Symbol           string  `mapstructure:"symbol"`
}

type ISmnLandItemMetadata struct {
	Categories  []any    `mapstructure:"categories"`
	Description string   `mapstructure:"description"`
	Name        string   `mapstructure:"name"`
	PreviewImg  string   `mapstructure:"preview_img"`
	Tags        []string `mapstructure:"tags"`
}

type ISmnLandItemOwner struct {
	Address  string `mapstructure:"address"`
	Username string `mapstructure:"username"`
}

type ISmnLandItemSellOrder struct {
	ForSale       int    `mapstructure:"for_sale"`
	ListedDaysAgo int    `mapstructure:"listed_days_ago"`
	Name          string `mapstructure:"name"`
	Price         int    `mapstructure:"price"`
	PriceInEth    int    `mapstructure:"price_in_eth"`
	PriceInUsd    int    `mapstructure:"price_in_usd"`
	Symbol        string `mapstructure:"symbol"`
}

type ISmnLandItem struct {
	Coordinates     *ISmnLandItemCoordinate `mapstructure:"coordinates"`
	DefaultValue    int                     `mapstructure:"default_value"`
	ExternalLink    string                  `mapstructure:"external_link"`
	Geometry        map[string]any          `mapstructure:"geometry"`
	ImagePreviewUrl string                  `mapstructure:"image_preview_url"`
	Index           int                     `mapstructure:"index"`
	LastSale        *ISmnLandItemLastSale   `mapstructure:"last_sale"`
	Metadata        *ISmnLandItemMetadata   `mapstructure:"metadata"`
	NumSales        int                     `mapstructure:"num_sales"`
	Owner           *ISmnLandItemOwner      `mapstructure:"owner"`
	ParcelCount     int                     `mapstructure:"parcel_count"`
	ParcelLocation  string                  `mapstructure:"parcel_location"`
	ParcelSize      string                  `mapstructure:"parcel_size"`
	ParcelSizeM     int                     `mapstructure:"parcel_size_m"`
	Permalink       string                  `mapstructure:"permalink"`
	SellOrder       *ISmnLandItemSellOrder  `mapstructure:"sell_order"`
	TokenId         string                  `mapstructure:"token_id"`
	Updated         int                     `mapstructure:"updated"`
}

type ISmnLandItemList struct {
	Refreshed string          `mapstructure:"refreshed"`
	Assets    []*ISmnLandItem `mapstructure:"assets"`
}

type ISmnWorldAttribute struct {
	DisplayType string `mapstructure:"display_type"`
	TraitType   string `mapstructure:"trait_type"`
	Value       string `mapstructure:"value"`
}

type ISmnWorldItem struct {
	Attributes           []*ISmnWorldAttribute `mapstructure:"attributes"`
	Description          string                `mapstructure:"description"`
	ExternalUrl          string                `mapstructure:"external_url"`
	Image                string                `mapstructure:"image"`
	Name                 string                `mapstructure:"name"`
	AnimationUrl         string                `mapstructure:"animation_url"`
	YoutubeUrl           string                `mapstructure:"youtube_url"`
	SellerFeeBasisPoints float64               `mapstructure:"seller_fee_basis_points"`
	Collection           any                   `mapstructure:"collection"`
	Properties           any                   `mapstructure:"properties"`
}

func getSmnLandItemList() (*ISmnLandItemList, error) {
	url := "https://map.somniumspace.com/data/parcels.json"
	jsonResp := make(map[string]any)
	err := helpers.FetchData(url, "", &jsonResp)
	if err != nil {
		return nil, err
	}
	rawLandsList := ISmnLandItemList{}
	err = helpers.ConvertMapToStruct(jsonResp, &rawLandsList)
	if err != nil {
		return nil, err
	}
	return &rawLandsList, nil
}

func getSmnLandInfo(landId int) (*ISmdLand, error) {
	url := fmt.Sprintf("https://parcels.somniumspace.org/parcels/api/Parcels/%d", landId)
	jsonResp := make(map[string]any)
	apiKey := fmt.Sprintf("Bearer %s", os.Getenv("SOMNIUM_SPACE_AUTH_TOKEN"))
	err := helpers.FetchData(url, apiKey, &jsonResp)
	if err != nil {
		return nil, err
	}
	rawLandInfo := ISmdLand{}
	err = helpers.ConvertMapToStruct(jsonResp, &rawLandInfo)
	if err != nil {
		return nil, err
	}
	return &rawLandInfo, nil
}

func getSmnWorldItem(worldId int) (*ISmnWorldItem, error) {
	url := fmt.Sprintf("https://somnium.space/parcel/api/Parcels/SomniumSpaceWorld/%d", worldId)
	jsonResp := make(map[string]any)
	err := helpers.FetchData(url, "", &jsonResp)
	if err != nil {
		return nil, err
	}
	rawWorldItem := ISmnWorldItem{}
	err = helpers.ConvertMapToStruct(jsonResp, &rawWorldItem)
	if err != nil {
		return nil, err
	}
	return &rawWorldItem, nil
}

func findSmnWorldAttributeValue(attrs []*ISmnWorldAttribute, attrName string) string {
	for _, attr := range attrs {
		if attr.TraitType == attrName {
			return attr.Value
		}
	}
	return ""
}

func parseSmnSize(strSize string) int {
	switch strSize {
	case "S":
		return 1
	case "M":
		return 2
	case "XL":
		return 3
	default:
		return 1
	}
}

func parseSmnLand(mtvInfo *MetaverseInfo, rawLandInfo *ISmdLand, rawLandItem *ISmnLandItem) *MetaverseAsset {
	mtvInfoAsset := getMetaverseInfoAsset(mtvInfo, "land", EthereumBlockchain)
	description := ""
	if rawLandItem.Metadata != nil {
		description = rawLandItem.Metadata.Description
	}
	asset := &MetaverseAsset{
		Metaverse:     string(MetaverseSmn),
		Blockchain:    EthereumBlockchain,
		Contract:      mtvInfoAsset.Contract,
		TokenStandard: "erc721",
		AssetId:       strconv.Itoa(rawLandInfo.Id),
		AssetType:     MtvAssetTypeRealEstate,
		AssetSubtype:  MtvAssetStypeRELand,
		Name:          rawLandInfo.Name,
		Description:   description,
		Location:      fmt.Sprintf("%f,%f,%f", rawLandInfo.Location.X, rawLandInfo.Location.Y, rawLandInfo.Location.Z),
		Size:          float64(parseSmnSize(rawLandInfo.Size)),
		Details: map[string]any{
			"info": *rawLandInfo,
			"item": *rawLandItem,
		},
	}
	asset.CreatedAt = time.Now()
	asset.UpdatedAt = time.Now()
	return asset
}

func parseSmnWorld(mtvInfo *MetaverseInfo, rawWorldItem *ISmnWorldItem, worldId int) *MetaverseAsset {
	mtvInfoAsset := getMetaverseInfoAsset(mtvInfo, "world", EthereumBlockchain)
	details := make(map[string]any)
	_ = helpers.ConvertStructToMap(rawWorldItem, &details)
	asset := &MetaverseAsset{
		Metaverse:     string(MetaverseSmn),
		Blockchain:    EthereumBlockchain,
		Contract:      mtvInfoAsset.Contract,
		TokenStandard: "erc721",
		AssetId:       strconv.Itoa(worldId),
		AssetType:     MtvAssetTypeService,
		AssetSubtype:  MtvAssetStypeSrSpace,
		Name:          rawWorldItem.Name,
		Description:   rawWorldItem.Description,
		Location:      "",
		Size:          float64(parseSmnSize(findSmnWorldAttributeValue(rawWorldItem.Attributes, "WORLD SIZE"))),
		Details:       details,
	}
	asset.CreatedAt = time.Now()
	asset.UpdatedAt = time.Now()
	return asset
}

func processSmnTaskLand(task string, params map[string]any, dbInstance *mongo.Database) ([]*MetaverseAsset, error) {
	metaverseInfo := params["metaverseInfo"].(*MetaverseInfo)
	smnLandItemsList := params["landItemsList"].(*ISmnLandItemList)

	landIndex, _ := strconv.Atoi(strings.Split(task, ":")[2])
	rawLandItem := smnLandItemsList.Assets[landIndex]
	landId, _ := strconv.Atoi(rawLandItem.TokenId)

	rawLandInfo, err := getSmnLandInfo(landId)
	if err != nil {
		return nil, err
	}

	asset := parseSmnLand(metaverseInfo, rawLandInfo, rawLandItem)

	return []*MetaverseAsset{asset}, nil
}

func processSmnTaskWorld(task string, params map[string]any, dbInstance *mongo.Database) ([]*MetaverseAsset, error) {
	metaverseInfo := params["metaverseInfo"].(*MetaverseInfo)

	worldId, _ := strconv.Atoi(strings.Split(task, ":")[2])
	rawWorldItem, err := getSmnWorldItem(worldId)
	if err != nil {
		return nil, err
	}

	asset := parseSmnWorld(metaverseInfo, rawWorldItem, worldId)

	return []*MetaverseAsset{asset}, nil
}

func processSmnTaskAsset(task string, params map[string]any, dbInstance *mongo.Database) ([]*MetaverseAsset, error) {
	assetType := strings.Split(task, ":")[1]
	if assetType == "land" {
		return processSmnTaskLand(task, params, dbInstance)
	} else {
		return processSmnTaskWorld(task, params, dbInstance)
	}
}

func processSmnTaskExtra(task string, params map[string]any, dbInstance *mongo.Database) error {
	return nil
}

func getterSmnRequestsOrder() ([]string, error) {
	reqOrder := make([]string, 0)

	parcelsCount := 5000
	worldsCount := 186
	for i := 0; i < parcelsCount; i++ {
		reqOrder = append(reqOrder, fmt.Sprintf("asset:land:%d", i))
	}
	for i := 0; i < worldsCount; i++ {
		reqOrder = append(reqOrder, fmt.Sprintf("asset:world:%d", i+1))
	}

	return reqOrder, nil
}
