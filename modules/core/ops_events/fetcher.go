package ops_events

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
)

func getEventsFromOpensea(collection collections.Collection, nextToken string) (*helpers.OpenSeaListResponse, error) {
	url := fmt.Sprintf("https://api.opensea.io/api/v2/events/collection/%s?event_type=sale&event_type=transfer&limit=%d", string(collection), helpers.OpenSeaListLimitEvents)
	if nextToken != "" {
		url = fmt.Sprintf("%s&next=%s", url, nextToken)
	}
	return helpers.GetListOpenseaData(url)
}
