package assets

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
)

func getAssetFromOpensea(collection collections.Collection, nextToken string) (*helpers.OpenSeaListResponse, error) {
	url := fmt.Sprintf("https://api.opensea.io/api/v2/collection/%s/nfts?limit=%d", string(collection), helpers.OpenSeaListLimit)
	if nextToken != "" {
		url = fmt.Sprintf("%s&next=%s", url, nextToken)
	}
	return helpers.GetListOpenseaData(url)
}
