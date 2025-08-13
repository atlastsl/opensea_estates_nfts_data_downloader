package metaverses

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"github.com/kamva/mgm/v3"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
	"os"
	"path/filepath"
	"reflect"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"
)

type ISndEstatePreview struct {
	EstateId         int    `mapstructure:"estateId"`
	PreviewHash      string `mapstructure:"previewHash"`
	PreviewExtension string `mapstructure:"previewExtension"`
	Order            int    `mapstructure:"order"`
}

type ISndUserSocialNetwork struct {
	Platform    string `mapstructure:"platform"`
	UserId      string `mapstructure:"userId"`
	UrlOrHandle string `mapstructure:"urlOrHandle"`
}

type ISndUser struct {
	Id                string                   `mapstructure:"id"`
	Username          string                   `mapstructure:"username"`
	AvatarHash        string                   `mapstructure:"avatarHash"`
	AvatarExtension   string                   `mapstructure:"avatarExtension"`
	UserSocialNetwork []*ISndUserSocialNetwork `mapstructure:"UserSocialNetworkUrls"`
}

type ISndWallet struct {
	Address string    `mapstructure:"address"`
	User    *ISndUser `mapstructure:"user"`
}

type ISndEstate struct {
	Id             int                  `mapstructure:"id"`
	Name           string               `mapstructure:"name"`
	Description    string               `mapstructure:"description"`
	CoordinateX    int                  `mapstructure:"coordinateX"`
	CoordinateY    int                  `mapstructure:"coordinateY"`
	Type           int                  `mapstructure:"type"`
	OwnerAddress   string               `mapstructure:"ownerAddress"`
	Sector         int                  `mapstructure:"sector"`
	LogoHash       string               `mapstructure:"logoHash"`
	LogoExtension  string               `mapstructure:"logoExtension"`
	IsComplete     bool                 `mapstructure:"isComplete"`
	Url            string               `mapstructure:"url"`
	CreatedAt      string               `mapstructure:"createdAt"`
	UpdatedAt      string               `mapstructure:"updatedAt"`
	DeletedAt      string               `mapstructure:"deletedAt"`
	VideoUrl       string               `mapstructure:"videoUrl"`
	EstatePreviews []*ISndEstatePreview `mapstructure:"EstatePreviews"`
	Wallet         *ISndWallet          `mapstructure:"wallet"`
}

type ISndOrderLand struct {
	Id          string `mapstructure:"id"`
	Name        string `mapstructure:"name"`
	CoordinateX int    `mapstructure:"coordinateX"`
	CoordinateY int    `mapstructure:"coordinateY"`
	ImageUrl    string `mapstructure:"imageUrl"`
	IsPremium   bool   `mapstructure:"isPremium"`
}

type ISndOrder struct {
	Canceling       bool           `mapstructure:"canceling"`
	EndDate         int64          `mapstructure:"endDate"`
	Source          string         `mapstructure:"source"`
	OwnerWalletId   int            `mapstructure:"ownerWalletId"`
	NeighborhoodId  int            `mapstructure:"neighborhoodId"`
	CoordinateX     int            `mapstructure:"coordinateX"`
	CoordinateY     int            `mapstructure:"coordinateY"`
	ChainId         int            `mapstructure:"chainId"`
	Price           string         `mapstructure:"price"`
	Buying          bool           `mapstructure:"buying"`
	Currency        string         `mapstructure:"currency"`
	NormalizedPrice float64        `mapstructure:"normalizedPrice"`
	IsPremium       bool           `mapstructure:"isPremium"`
	StartDate       int64          `mapstructure:"startDate"`
	UpdatedAt       int64          `mapstructure:"updatedAt"`
	Land            *ISndOrderLand `mapstructure:"land"`
}

type ISndLand struct {
	Id               string       `mapstructure:"id"`
	Name             string       `mapstructure:"name"`
	Description      string       `mapstructure:"description"`
	CoordinateX      int          `mapstructure:"coordinateX"`
	CoordinateY      int          `mapstructure:"coordinateY"`
	PreviewHash      string       `mapstructure:"previewHash"`
	PreviewExtension string       `mapstructure:"previewExtension"`
	LogoHash         string       `mapstructure:"logoHash"`
	LogoExtension    string       `mapstructure:"logoExtension"`
	ContentHash      string       `mapstructure:"contentHash"`
	BlockchainId     string       `mapstructure:"blockchainId"`
	Sector           int          `mapstructure:"sector"`
	Url              string       `mapstructure:"url"`
	VideoUrl         string       `mapstructure:"videoUrl"`
	Migrated         bool         `mapstructure:"migrated"`
	PartnerId        int          `mapstructure:"partnerId"`
	ChainIdChangedAt string       `mapstructure:"chainIdChangedAt"`
	CreatedAt        string       `mapstructure:"createdAt"`
	UpdatedAt        string       `mapstructure:"updatedAt"`
	DeletedAt        string       `mapstructure:"deletedAt"`
	OwnerAddress     string       `mapstructure:"ownerAddress"`
	Estate           int          `mapstructure:"estate"`
	BundleId         int          `mapstructure:"bundleId"`
	ChainId          int          `mapstructure:"chainId"`
	LastIndexedAt    string       `mapstructure:"lastIndexedAt"`
	NeighborhoodId   int          `mapstructure:"neighborhoodId"`
	IEstate          *ISndEstate  `mapstructure:"Estate"`
	Wallet           *ISndWallet  `mapstructure:"wallet"`
	Orders           []*ISndOrder `mapstructure:"orders"`
	HasMapExperience bool         `mapstructure:"hasMapExperience"`
}

type ISndLandList struct {
	Lands []*ISndLand `mapstructure:"data"`
}

type ISndXpOwner struct {
	CreatorType string `mapstructure:"creatorType"`
	Id          string `mapstructure:"id"`
	Username    string `mapstructure:"username"`
}

type ISndXpEstate struct {
	CoordinateX      int    `mapstructure:"coordinateX"`
	CoordinateY      int    `mapstructure:"coordinateY"`
	Id               int    `mapstructure:"id"`
	Type             int    `mapstructure:"type"`
	LogoExtension    string `mapstructure:"logoExtension"`
	Sector           int    `mapstructure:"sector"`
	LogoHash         string `mapstructure:"logoHash"`
	IsComplete       bool   `mapstructure:"isComplete"`
	X                int    `mapstructure:"x"`
	Y                int    `mapstructure:"y"`
	TotalExperiences int    `mapstructure:"totalExperiences"`
}

type ISndXpEvent struct {
	EndDate   string `mapstructure:"endDate"`
	Id        int    `mapstructure:"id"`
	Permalink string `mapstructure:"permalink"`
	StartDate string `mapstructure:"startDate"`
	Title     string `mapstructure:"title"`
}

type ISndExperience struct {
	HubId              string           `mapstructure:"hubId"`
	Featured           string           `mapstructure:"featured"`
	Owner              *ISndXpOwner     `mapstructure:"owner"`
	PublishDate        string           `mapstructure:"publish_date"`
	Rating             float64          `mapstructure:"rating"`
	EhvStatus          string           `mapstructure:"ehvStatus"`
	Description        string           `mapstructure:"description"`
	SecondaryGenre     int              `mapstructure:"secondaryGenre"`
	Visits             any              `mapstructure:"visits"`
	Events             []*ISndXpEvent   `mapstructure:"events"`
	Genre              int              `mapstructure:"genre"`
	Theme              int              `mapstructure:"theme"`
	Id                 string           `mapstructure:"id"`
	MinPlayer          int              `mapstructure:"minPlayer"`
	MaxPlayer          int              `mapstructure:"maxPlayer"`
	Thumbnail          string           `mapstructure:"thumbnail"`
	Quests             int              `mapstructure:"quests"`
	UnpublishDate      string           `mapstructure:"unpublishDate"`
	Banner             string           `mapstructure:"banner"`
	PageId             string           `mapstructure:"pageId"`
	PageIsLive         bool             `mapstructure:"pageIsLive"`
	SizeX              int              `mapstructure:"sizeX"`
	SizeY              int              `mapstructure:"sizeY"`
	VersionId          string           `mapstructure:"versionId"`
	SpecialAbilities   []map[string]any `mapstructure:"specialAbilities"`
	MinX               int              `mapstructure:"minX"`
	minY               int              `mapstructure:"minY"`
	Name               string           `mapstructure:"name"`
	AccessRequirements bool             `mapstructure:"accessRequirements"`
	GameVersion        int              `mapstructure:"gameVersion"`
	MapLogo            string           `mapstructure:"mapLogo"`
	GameMode           int              `mapstructure:"gameMode"`
	GallerySections    []map[string]any `mapstructure:"gallerySections"`
	Status             string           `mapstructure:"status"`
	ExperienceId       string           `mapstructure:"experienceId"`
	LandId             string           `mapstructure:"landId"`
	X                  int              `mapstructure:"x"`
	Y                  int              `mapstructure:"y"`
	Premium            bool             `mapstructure:"premium"`
	Estate             *ISndXpEstate    `mapstructure:"estate"`
	EstateId           int              `mapstructure:"estateId"`
}

type ISndExperienceList struct {
	Experiences []*ISndExperience `mapstructure:"data"`
}

type ISndNeighborhood struct {
	Key          string  `mapstructure:"key"`
	Neighborhood int     `mapstructure:"neighborhood"`
	Perimeter    [][]int `mapstructure:"perimeter"`
}

type ISndNeighborhoodList struct {
	Neighborhoods []*ISndNeighborhood `mapstructure:"neighborhoods"`
}

type ISndXpGenre struct {
	Id       int    `mapstructure:"id"`
	Name     string `mapstructure:"name"`
	Position int    `mapstructure:"position"`
	Preview  string `mapstructure:"preview"`
	Type     string `mapstructure:"type"`
	Video    string `mapstructure:"video"`
}

type ISndXpGenreList struct {
	Categories []*ISndXpGenre `mapstructure:"categories"`
}

type ISndXpTheme struct {
	Id       int    `mapstructure:"id"`
	Name     string `mapstructure:"name"`
	Position int    `mapstructure:"position"`
	Preview  string `mapstructure:"preview"`
	Video    string `mapstructure:"video"`
}

type ISndXpThemeList struct {
	Themes []*ISndXpTheme `mapstructure:"themes"`
}

type TheSandboxLand struct {
	Id               string         `json:"id"`
	BlockchainId     string         `json:"blockchain_id"`
	Name             string         `json:"name"`
	Description      string         `json:"description"`
	X                int            `json:"x"`
	Y                int            `json:"y"`
	PreviewHash      string         `json:"preview_hash"`
	PreviewExtension string         `json:"preview_extension"`
	LogoHash         string         `json:"logo_hash"`
	LogoExtension    string         `json:"logo_extension"`
	Blockchain       string         `json:"blockchain"`
	Sector           int            `json:"sector"`
	Url              string         `json:"url"`
	VideoUrl         string         `json:"video_url"`
	PartnerId        int            `json:"partner_id"`
	ChainIdChangedAt time.Time      `json:"chain_id_changed_at"`
	CreatedAt        time.Time      `json:"created_at"`
	UpdatedAt        time.Time      `json:"updated_at"`
	DeletedAt        time.Time      `json:"deleted_at"`
	OwnerAddress     string         `json:"owner_address"`
	OwnerId          string         `json:"owner_id"`
	OwnerName        string         `json:"owner_name"`
	Estate           int            `json:"estate"`
	Neighborhood     int            `json:"neighborhood"`
	RawLand          map[string]any `json:"raw_land"`
}

type TheSandboxEstate struct {
	mgm.DefaultModel       `bson:",inline,omitempty"`
	Id                     int            `bson:"id,omitempty"`
	Name                   string         `bson:"name,omitempty"`
	Type                   int            `bson:"type,omitempty"`
	X                      int            `bson:"x"`
	Y                      int            `bson:"y"`
	Size                   int            `bson:"size,omitempty"`
	LogoHash               string         `bson:"logo_hash,omitempty"`
	LogoExtension          string         `bson:"logo_extension,omitempty"`
	IsComplete             bool           `bson:"is_complete,omitempty"`
	Url                    string         `bson:"url,omitempty"`
	CreatedAt              time.Time      `bson:"created_at,omitempty"`
	UpdatedAt              time.Time      `bson:"updated_at,omitempty"`
	DeletedAt              time.Time      `bson:"deleted_at,omitempty"`
	VideoUrl               string         `bson:"video_url,omitempty"`
	EstatePreviewHash      string         `bson:"estate_preview_hash,omitempty"`
	EstatePreviewExtension string         `bson:"estate_preview_extension,omitempty"`
	OwnerAddress           string         `bson:"owner_address,omitempty"`
	OwnerId                string         `bson:"owner_id,omitempty"`
	OwnerName              string         `bson:"owner_name,omitempty"`
	RawEstate              map[string]any `bson:"raw_estate,omitempty"`
}

type TheSandboxOrder struct {
	mgm.DefaultModel `bson:",inline,omitempty"`
	Canceling        bool           `bson:"canceling"`
	EndDate          time.Time      `bson:"end_date,omitempty"`
	Source           string         `bson:"source,omitempty"`
	Neighborhood     int            `bson:"neighborhood,omitempty"`
	LandX            int            `bson:"land_x"`
	LandY            int            `bson:"land_y"`
	LandId           string         `bson:"land_id,omitempty"`
	Blockchain       string         `bson:"blockchain,omitempty"`
	Price            float64        `bson:"price,omitempty"`
	Currency         string         `bson:"currency,omitempty"`
	PriceUsd         float64        `bson:"price_usd,omitempty"`
	Buying           bool           `bson:"buying"`
	IsPremium        bool           `bson:"is_premium"`
	StartDate        time.Time      `bson:"start_date,omitempty"`
	UpdatedAt        time.Time      `bson:"updated_at,omitempty"`
	RawOrderData     map[string]any `bson:"raw_order_data,omitempty"`
}

type TheSandboxExperience struct {
	mgm.DefaultModel `bson:",inline,omitempty"`
	ExperienceId     string         `bson:"experience_id,omitempty"`
	HubId            string         `bson:"hub_id,omitempty"`
	PageId           string         `bson:"page_id,omitempty"`
	Featured         *time.Time     `bson:"featured,omitempty"`
	PublishDate      *time.Time     `bson:"publish_date,omitempty"`
	UnpublishDate    *time.Time     `bson:"unpublish_date,omitempty"`
	Name             string         `bson:"name,omitempty"`
	Description      string         `bson:"description,omitempty"`
	OwnerCreatorType string         `bson:"owner_creator_type,omitempty"`
	OwnerAddress     string         `bson:"owner_address,omitempty"`
	OwnerId          string         `bson:"owner_id,omitempty"`
	OwnerName        string         `bson:"owner_name,omitempty"`
	Theme            string         `bson:"theme,omitempty"`
	Genre            string         `bson:"genre,omitempty"`
	Rating           float64        `bson:"rating,omitempty"`
	Status           string         `bson:"status,omitempty"`
	EvhStatus        string         `bson:"evh_status,omitempty"`
	Visits           int            `bson:"visits"`
	PageIsLive       bool           `bson:"pageIsLive"`
	SizeX            int            `bson:"sizeX"`
	SizeY            int            `bson:"sizeY"`
	minX             int            `bson:"minX"`
	minY             int            `bson:"minY"`
	X                int            `bson:"x"`
	Y                int            `bson:"y"`
	Land             string         `bson:"land,omitempty"`
	Premium          bool           `bson:"premium"`
	Estate           int            `bson:"estate,omitempty"`
	XpRawData        map[string]any `bson:"xp_raw_data,omitempty"`
}

type TheSandboxNeighborhood struct {
	mgm.DefaultModel `bson:",inline,omitempty"`
	Key              string   `bson:"key,omitempty"`
	Id               int      `bson:"id,omitempty"`
	Perimeter        []string `bson:"perimeter,omitempty"`
}

func sndReqApi(url string, maxRetries int, target any) error {
	nbTries := 0
	for nbTries < maxRetries {
		err := helpers.FetchData(url, "", target)
		if err != nil {
			if strings.Contains(err.Error(), "400") {
				return nil
			} else if !strings.Contains(err.Error(), "500") {
				return err
			}
		} else {
			return nil
		}
		nbTries++
		time.Sleep(1 * time.Second)
	}
	return errors.New(fmt.Sprintf("failed after %d retries", maxRetries))
}

func getSndLandInfo(x, y int) (*ISndLand, error) {
	url := fmt.Sprintf("https://api.sandbox.game/lands/coordinates?coordinateX={%d}&coordinateY={%d}&includeExperience=true&includeWallet=true&includeNft=true", x, y)
	jsonResp := make(map[string]any)
	err := sndReqApi(url, 5, &jsonResp)
	if err != nil {
		return nil, err
	}
	rawLand := &ISndLand{}
	err = helpers.ConvertMapToStruct(jsonResp, rawLand)
	if err != nil {
		return nil, err
	}
	return rawLand, nil
}

func getSndExperiences(page int) (*ISndExperienceList, error) {
	url := fmt.Sprintf("https://api.sandbox.game/lands/es/map-list/%d?sortBy=new", page)
	jsonResp := make(map[string]any)
	err := sndReqApi(url, 3, &jsonResp)
	if err != nil {
		return nil, err
	}
	rawExpList := &ISndExperienceList{}
	err = helpers.ConvertMapToStruct(jsonResp, rawExpList)
	if err != nil {
		return nil, err
	}
	return rawExpList, nil
}

func getSndNeighborhoodList() (*ISndNeighborhoodList, error) {
	url := "https://api.sandbox.game/map/neighborhoods-perimeter"
	jsonResp := make([]any, 0)
	err := sndReqApi(url, 3, &jsonResp)
	if err != nil {
		return nil, err
	}
	cJsonResp := map[string]any{
		"neighborhoods": jsonResp,
	}
	rawNglList := ISndNeighborhoodList{}
	err = helpers.ConvertMapToStruct(cJsonResp, &rawNglList)
	if err != nil {
		return nil, err
	}
	return &rawNglList, nil
}

func getSndGenreList() (*ISndXpGenreList, error) {
	url := "https://api.sandbox.game/experience-categories/gallery-genre-list?page=1&limit=100"
	jsonResp := make(map[string]any)
	err := sndReqApi(url, 3, &jsonResp)
	if err != nil {
		return nil, err
	}
	rawGnrList := &ISndXpGenreList{}
	err = helpers.ConvertMapToStruct(jsonResp, rawGnrList)
	if err != nil {
		return nil, err
	}
	return rawGnrList, nil
}

func getSndThemeList() (*ISndXpThemeList, error) {
	url := "https://api.sandbox.game/experience-categories/gallery-theme-list"
	jsonResp := make([]any, 0)
	err := sndReqApi(url, 3, &jsonResp)
	if err != nil {
		return nil, err
	}
	cJsonResp := map[string]any{
		"themes": jsonResp,
	}
	rawThmList := &ISndXpThemeList{}
	err = helpers.ConvertMapToStruct(cJsonResp, &rawThmList)
	if err != nil {
		return nil, err
	}
	return rawThmList, nil
}

func getSndLandsInfoLocal(fileName string) ([]*ISndLand, error) {
	filePath := filepath.Join(os.Getenv("THESANDBOX_DATA_PATH"), fileName)
	var rawLandsList ISndLandList
	err := helpers.ReadJsonFile(filePath, &rawLandsList)
	if err != nil {
		return nil, err
	}
	return rawLandsList.Lands, nil
}

func getSndExperiencesLocal(fileName string) ([]*ISndExperience, error) {
	filePath := filepath.Join(os.Getenv("THESANDBOX_DATA_PATH"), fileName)
	var rawExperiencesList ISndExperienceList
	err := helpers.ReadJsonFile(filePath, &rawExperiencesList)
	if err != nil {
		return nil, err
	}
	return rawExperiencesList.Experiences, nil
}

func parseSndBlockchainName(chainId int) string {
	switch chainId {
	case 1:
		return EthereumBlockchain
	default:
		return PolygonBlockchain
	}
}

func parseSndLandInfo(sndMtvInfo *MetaverseInfo, rawLand *ISndLand) (*MetaverseAsset, *TheSandboxEstate, []*TheSandboxOrder) {
	var asset *MetaverseAsset
	var pLand *TheSandboxLand
	var pEstate *TheSandboxEstate
	var pOrders = make([]*TheSandboxOrder, 0)
	if rawLand != nil {
		ownerId, ownerName := "", ""
		if rawLand.Wallet != nil && rawLand.Wallet.User != nil {
			ownerId = rawLand.Wallet.User.Id
			ownerName = rawLand.Wallet.User.Username
		}
		chChDate, crtDate, uptDate, dltDate := time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)
		if rawLand.ChainIdChangedAt != "" {
			chChDate, _ = time.Parse(time.RFC3339Nano, rawLand.ChainIdChangedAt)
		}
		if rawLand.CreatedAt != "" {
			crtDate, _ = time.Parse(time.RFC3339Nano, rawLand.CreatedAt)
		}
		if rawLand.UpdatedAt != "" {
			uptDate, _ = time.Parse(time.RFC3339Nano, rawLand.UpdatedAt)
		}
		if rawLand.DeletedAt != "" {
			dltDate, _ = time.Parse(time.RFC3339Nano, rawLand.DeletedAt)
		}
		rawLandMap := map[string]any{}
		_ = helpers.ConvertStructToMap(rawLand, &rawLandMap)
		pLand = &TheSandboxLand{
			Id:               rawLand.Id,
			BlockchainId:     rawLand.BlockchainId,
			Name:             rawLand.Name,
			Description:      rawLand.Description,
			X:                rawLand.CoordinateX,
			Y:                rawLand.CoordinateY,
			PreviewHash:      rawLand.PreviewHash,
			PreviewExtension: rawLand.PreviewExtension,
			LogoHash:         rawLand.LogoHash,
			LogoExtension:    rawLand.LogoExtension,
			Blockchain:       parseSndBlockchainName(rawLand.ChainId),
			Sector:           rawLand.Sector,
			Url:              rawLand.Url,
			VideoUrl:         rawLand.VideoUrl,
			PartnerId:        rawLand.PartnerId,
			ChainIdChangedAt: chChDate,
			CreatedAt:        crtDate,
			UpdatedAt:        uptDate,
			DeletedAt:        dltDate,
			OwnerAddress:     rawLand.OwnerAddress,
			OwnerId:          ownerId,
			OwnerName:        ownerName,
			Estate:           rawLand.Estate,
			Neighborhood:     rawLand.NeighborhoodId,
			RawLand:          rawLandMap,
		}
		mtvAssetDetailsMap := map[string]any{}
		_ = helpers.ConvertStructToMap(pLand, &mtvAssetDetailsMap)
		assetInfo := getMetaverseInfoAsset(sndMtvInfo, "land", parseSndBlockchainName(rawLand.ChainId))
		asset = &MetaverseAsset{
			Metaverse:     string(MetaverseSnd),
			Blockchain:    parseSndBlockchainName(rawLand.ChainId),
			Contract:      assetInfo.Contract,
			TokenStandard: "erc721",
			AssetId:       pLand.BlockchainId,
			AssetType:     MtvAssetTypeRealEstate,
			AssetSubtype:  MtvAssetStypeRELand,
			Name:          pLand.Name,
			Description:   pLand.Description,
			Location:      fmt.Sprintf("%d,%d", pLand.X, pLand.Y),
			Size:          1,
			Details:       mtvAssetDetailsMap,
		}
		asset.CreatedAt = pLand.CreatedAt
		asset.UpdatedAt = pLand.UpdatedAt
		if rawLand.IEstate != nil {
			epHash, epExt, ownId, ownName := "", "", "", ""
			if rawLand.IEstate.EstatePreviews != nil && len(rawLand.IEstate.EstatePreviews) > 0 {
				epHash = rawLand.IEstate.EstatePreviews[0].PreviewHash
				epExt = rawLand.IEstate.EstatePreviews[0].PreviewExtension
			}
			if rawLand.IEstate.Wallet != nil && rawLand.IEstate.Wallet.User != nil {
				ownId = rawLand.IEstate.Wallet.User.Id
				ownName = rawLand.IEstate.Wallet.User.Username
			}
			eCrtDate, eUptDate, eDltDate := time.Unix(0, 0), time.Unix(0, 0), time.Unix(0, 0)
			if rawLand.CreatedAt != "" {
				eCrtDate, _ = time.Parse(time.RFC3339Nano, rawLand.CreatedAt)
			}
			if rawLand.UpdatedAt != "" {
				eUptDate, _ = time.Parse(time.RFC3339Nano, rawLand.UpdatedAt)
			}
			if rawLand.DeletedAt != "" {
				eDltDate, _ = time.Parse(time.RFC3339Nano, rawLand.DeletedAt)
			}
			rawEstateMap := map[string]any{}
			_ = helpers.ConvertStructToMap(rawLand.IEstate, &rawEstateMap)
			pEstate = &TheSandboxEstate{
				Id:                     rawLand.IEstate.Id,
				Name:                   rawLand.IEstate.Name,
				Type:                   rawLand.IEstate.Type,
				X:                      rawLand.IEstate.CoordinateX,
				Y:                      rawLand.IEstate.CoordinateY,
				Size:                   int(3 * math.Pow(2.0, float64(rawLand.IEstate.Type))),
				LogoHash:               rawLand.IEstate.LogoHash,
				LogoExtension:          rawLand.IEstate.LogoExtension,
				IsComplete:             rawLand.IEstate.IsComplete,
				Url:                    rawLand.IEstate.Url,
				CreatedAt:              eCrtDate,
				UpdatedAt:              eUptDate,
				DeletedAt:              eDltDate,
				VideoUrl:               rawLand.IEstate.VideoUrl,
				EstatePreviewHash:      epHash,
				EstatePreviewExtension: epExt,
				OwnerAddress:           rawLand.IEstate.OwnerAddress,
				OwnerId:                ownId,
				OwnerName:              ownName,
				RawEstate:              rawEstateMap,
			}
		}
		if rawLand.Orders != nil && len(rawLand.Orders) > 0 {
			for _, order := range rawLand.Orders {
				fPrice, _ := strconv.ParseFloat(order.Price, 64)
				rawOrderMap := map[string]any{}
				_ = helpers.ConvertStructToMap(order, &rawOrderMap)
				pOrder := &TheSandboxOrder{
					Canceling:    order.Canceling,
					EndDate:      time.UnixMilli(order.EndDate),
					Source:       order.Source,
					Neighborhood: order.NeighborhoodId,
					LandX:        order.CoordinateX,
					LandY:        order.CoordinateY,
					LandId:       rawLand.BlockchainId,
					Blockchain:   parseSndBlockchainName(order.ChainId),
					Price:        fPrice,
					Currency:     order.Currency,
					PriceUsd:     order.NormalizedPrice,
					Buying:       order.Buying,
					IsPremium:    order.IsPremium,
					StartDate:    time.UnixMilli(order.StartDate),
					UpdatedAt:    time.UnixMilli(order.UpdatedAt),
					RawOrderData: rawOrderMap,
				}
				pOrders = append(pOrders, pOrder)
			}
		}
	}
	return asset, pEstate, pOrders
}

func parseGenreName(genreId int, genreList *ISndXpGenreList) string {
	for _, category := range genreList.Categories {
		if category.Id == genreId {
			return category.Name
		}
	}
	return ""
}

func parseThemeName(themeId int, themeList *ISndXpThemeList) string {
	for _, theme := range themeList.Themes {
		if theme.Id == themeId {
			return theme.Name
		}
	}
	return ""
}

var (
	landsMapping       map[string]string
	landsMappingLocker sync.RWMutex
)

func registeredLandMapping(landId string, landBlockchainId string) {
	landsMappingLocker.Lock()
	defer landsMappingLocker.Unlock()
	if landsMapping == nil {
		landsMapping = make(map[string]string)
	}
	landsMapping[landId] = landBlockchainId
}

func readLandMapping(landId string) (landBlockchainId string, ok bool) {
	landsMappingLocker.RLock()
	defer landsMappingLocker.RUnlock()
	ok = false
	if landsMapping == nil {
		landBlockchainId = ""
	} else {
		landBlockchainId, _ = landsMapping[landId]
		ok = true
	}
	return
}

var (
	processedEstates       []int
	processedEstatesLocker sync.RWMutex
)

func registerProcessedEstate(estateId int) {
	processedEstatesLocker.Lock()
	defer processedEstatesLocker.Unlock()
	if processedEstates == nil {
		processedEstates = make([]int, 0)
	}
	processedEstates = append(processedEstates, estateId)
}

func checkInProcessedEstates(estateId int) bool {
	processedEstatesLocker.RLock()
	defer processedEstatesLocker.RUnlock()
	if processedEstates != nil {
		return slices.Contains(processedEstates, estateId)
	}
	return false
}

func parseSndExperienceInfo(rawExperience *ISndExperience, themeList *ISndXpThemeList, genreList *ISndXpGenreList) *TheSandboxExperience {
	if rawExperience == nil {
		return nil
	}
	ownAdd, ownId, ownCT, ownName := "", "", "", ""
	if rawExperience.Owner != nil {
		ownId = rawExperience.Owner.Id
		ownCT = rawExperience.Owner.CreatorType
		ownName = rawExperience.Owner.Username
	}
	visits := 0
	if reflect.TypeOf(rawExperience.Visits).Kind() == reflect.String {
		visits, _ = strconv.Atoi(rawExperience.Visits.(string))
	} else if reflect.TypeOf(rawExperience.Visits).Kind() == reflect.Int {
		visits = rawExperience.Visits.(int)
	}
	landIdStr, ok := readLandMapping(rawExperience.LandId)
	if !ok {
		landIdStr = rawExperience.LandId
	}
	var ftDate *time.Time
	var pubDate *time.Time
	var upubDate *time.Time
	if rawExperience.Featured != "" {
		temp, err := time.Parse(time.RFC3339Nano, rawExperience.Featured)
		if err != nil {
			ftDate = &temp
		}
	}
	if rawExperience.PublishDate != "" {
		temp, err := time.Parse(time.RFC3339Nano, rawExperience.PublishDate)
		if err != nil {
			pubDate = &temp
		}
	}
	if rawExperience.UnpublishDate != "" {
		temp, err := time.Parse(time.RFC3339Nano, rawExperience.UnpublishDate)
		if err != nil {
			upubDate = &temp
		}
	}
	rawXpMap := map[string]any{}
	_ = helpers.ConvertStructToMap(rawExperience, &rawXpMap)
	return &TheSandboxExperience{
		ExperienceId:     rawExperience.ExperienceId,
		HubId:            rawExperience.HubId,
		PageId:           rawExperience.PageId,
		Featured:         ftDate,
		PublishDate:      pubDate,
		UnpublishDate:    upubDate,
		Name:             rawExperience.Name,
		Description:      rawExperience.Description,
		OwnerCreatorType: ownCT,
		OwnerAddress:     ownAdd,
		OwnerId:          ownId,
		OwnerName:        ownName,
		Theme:            parseThemeName(rawExperience.Theme, themeList),
		Genre:            parseGenreName(rawExperience.Genre, genreList),
		Rating:           rawExperience.Rating,
		Status:           rawExperience.Status,
		EvhStatus:        rawExperience.EhvStatus,
		Visits:           visits,
		PageIsLive:       rawExperience.PageIsLive,
		SizeX:            rawExperience.SizeX,
		SizeY:            rawExperience.SizeY,
		minX:             rawExperience.MinX,
		minY:             rawExperience.minY,
		X:                rawExperience.X,
		Y:                rawExperience.Y,
		Land:             landIdStr,
		Premium:          rawExperience.Premium,
		Estate:           rawExperience.EstateId,
		XpRawData:        rawXpMap,
	}
}

func parseSndNeighborhoodInfo(rawNeighborhood *ISndNeighborhood) *TheSandboxNeighborhood {
	if rawNeighborhood == nil {
		return nil
	}
	neighborhood := &TheSandboxNeighborhood{
		Key: rawNeighborhood.Key,
		Id:  rawNeighborhood.Neighborhood,
		Perimeter: helpers.ArrayMap(rawNeighborhood.Perimeter, func(t []int) (bool, string) {
			return true, fmt.Sprintf("%d,%d", t[0], t[1])
		}, true, ""),
	}
	neighborhood.CreatedAt = time.Now()
	neighborhood.UpdatedAt = time.Now()
	return neighborhood
}

func saveSndEstateInfo(estates []*TheSandboxEstate, dbInstance *mongo.Database) error {
	if estates != nil && len(estates) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TheSandboxEstate{})

		bdOperations := make([]mongo.WriteModel, len(estates))
		for i, estate := range estates {
			var filterPayload = bson.M{"id": estate.Id}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(estate).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func saveSndOrdersInfo(orders []*TheSandboxOrder, dbInstance *mongo.Database) error {
	if orders != nil && len(orders) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TheSandboxOrder{})

		bdOperations := make([]mongo.WriteModel, len(orders))
		for i, order := range orders {
			bdOperations[i] = mongo.NewInsertOneModel().SetDocument(order)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func saveSndExperiencesInfo(experiences []*TheSandboxExperience, dbInstance *mongo.Database) error {
	if experiences != nil && len(experiences) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TheSandboxExperience{})

		bdOperations := make([]mongo.WriteModel, len(experiences))
		for i, experience := range experiences {
			var filterPayload = bson.M{"experience_id": experience.ExperienceId, "hub_id": experience.HubId}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(experience).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func saveSndNeighborhoodsInfo(neighborhoods []*TheSandboxNeighborhood, dbInstance *mongo.Database) error {
	if neighborhoods != nil && len(neighborhoods) > 0 {
		dbCollection := database.CollectionInstance(dbInstance, &TheSandboxNeighborhood{})

		bdOperations := make([]mongo.WriteModel, len(neighborhoods))
		for i, neighborhood := range neighborhoods {
			var filterPayload = bson.M{"key": neighborhood.Key, "id": neighborhood.Id}
			bdOperations[i] = mongo.NewReplaceOneModel().SetFilter(filterPayload).SetReplacement(neighborhood).SetUpsert(true)
		}
		_, err := dbCollection.BulkWrite(context.Background(), bdOperations)
		return err

	}
	return nil
}

func processTaskAssetSolo(task string, params map[string]any, dbInstance *mongo.Database) ([]*MetaverseAsset, error) {
	metaverseInfo := params["metaverseInfo"].(*MetaverseInfo)

	rawLandsFileName := strings.Split(task, ":")[2]
	rawLands, err := getSndLandsInfoLocal(rawLandsFileName)
	if err != nil {
		return nil, err
	}

	assets := make([]*MetaverseAsset, 0)
	estates := make([]*TheSandboxEstate, 0)
	orders := make([]*TheSandboxOrder, 0)
	for _, rawLand := range rawLands {
		asset, pEstate, pOrders := parseSndLandInfo(metaverseInfo, rawLand)
		assets = append(assets, asset)
		registeredLandMapping(rawLand.Id, rawLand.BlockchainId)
		if pEstate != nil && !checkInProcessedEstates(pEstate.Id) {
			estates = append(estates, pEstate)
			registerProcessedEstate(pEstate.Id)
		}
		orders = append(orders, pOrders...)
	}

	err = saveSndEstateInfo(estates, dbInstance)
	if err != nil {
		return nil, err
	}
	err = saveSndOrdersInfo(orders, dbInstance)
	if err != nil {
		return nil, err
	}

	return assets, nil
}

func processTaskExperienceSolo(task string, params map[string]any, dbInstance *mongo.Database) error {
	themesList := params["themes"].(*ISndXpThemeList)
	genresList := params["genres"].(*ISndXpGenreList)
	rawXpFileName := strings.Split(task, ":")[2]

	rawExperiences, err := getSndExperiencesLocal(rawXpFileName)
	if err != nil {
		return err
	}

	pExperiences := make([]*TheSandboxExperience, len(rawExperiences))
	for i, experience := range rawExperiences {
		pExperiences[i] = parseSndExperienceInfo(experience, themesList, genresList)
	}

	err = saveSndExperiencesInfo(pExperiences, dbInstance)

	return err
}

func processTaskExtraSolo(task string, params map[string]any, dbInstance *mongo.Database) error {
	extraType := strings.Split(task, ":")[1]
	if extraType == "experience" {
		return processTaskExperienceSolo(task, params, dbInstance)
	} else {
		neighborhoodsList := params["neighborhoods"].(*ISndNeighborhoodList)
		pNghList := make([]*TheSandboxNeighborhood, len(neighborhoodsList.Neighborhoods))
		for i, neighborhood := range neighborhoodsList.Neighborhoods {
			pNghList[i] = parseSndNeighborhoodInfo(neighborhood)
		}
		err := saveSndNeighborhoodsInfo(pNghList, dbInstance)
		return err
	}
}

func getterSndRequestsOrder() ([]string, error) {
	reqOrder := make([]string, 0)

	//assetsTasks := make([]string, 0)
	//XMin, XMax, YMin, YMax := -204, 203, -204, -203
	//for i := XMin; i <= XMax; i++ {
	//	for j := YMin; j <= YMax; j++ {
	//		task := fmt.Sprintf("asset_land_%d,%d", i, j)
	//		assetsTasks = append(assetsTasks, task)
	//	}
	//}
	//if isTesting {
	//	assetsTasks = helpers.SlicePickNRandom(assetsTasks, 100)
	//}
	//
	//experiencesTasks := make([]string, 0)
	//xpPageMin, xpPageMax := 1, 83
	//if isTesting {
	//	xpPageMax = 4
	//}
	//for i := xpPageMin; i <= xpPageMax; i++ {
	//	task := fmt.Sprintf("extra_experience_%d", i)
	//	experiencesTasks = append(experiencesTasks, task)
	//}

	parcelsFiles, err := helpers.ListFiles(os.Getenv("THESANDBOX_DATA_PATH"), "lands_raw_*.json", false)
	if err != nil {
		return nil, err
	}
	experiencesFiles, err := helpers.ListFiles(os.Getenv("THESANDBOX_DATA_PATH"), "experiences_raw_*.json", false)
	if err != nil {
		return nil, err
	}
	reqOrder = append(reqOrder, "extra:neighborhood")
	for _, file := range parcelsFiles {
		reqOrder = append(reqOrder, fmt.Sprintf("asset:land:%s", file))
	}
	for _, file := range experiencesFiles {
		reqOrder = append(reqOrder, fmt.Sprintf("extra:experience:%s", file))
	}

	return reqOrder, nil
}
