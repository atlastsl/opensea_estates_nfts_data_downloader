package helpers

import (
	"encoding/json"
	"io"
	"net/http"
	"os"
)

type OpenseaNftAsset struct {
	Identifier          *string `json:"identifier"`
	Collection          *string `json:"collection"`
	Contract            *string `json:"contract"`
	TokenStandard       *string `json:"token_standard"`
	Name                *string `json:"name"`
	Description         *string `json:"description"`
	ImageUrl            *string `json:"image_url"`
	DisplayImageUrl     *string `json:"display_image_url"`
	DisplayAnimationUrl *string `json:"display_animation_url"`
	MetadataUrl         *string `json:"metadata_url"`
	OpenseaUrl          *string `json:"opensea_uri"`
	UpdatedAt           *string `json:"updated_at"`
	IsDisabled          *bool   `json:"is_disabled"`
	IsNSFW              *bool   `json:"is_nsfw"`
}

type OpenseaNftEventPayment struct {
	Quantity     *string `json:"quantity"`
	TokenAddress *string `json:"token_address"`
	Decimals     *int    `json:"decimals"`
	Symbol       *string `json:"symbol"`
}

type OpenseaNftEvent struct {
	EventType       *string                 `json:"event_type"`
	OrderHash       *string                 `json:"order_hash"`
	ProtocolAddress *string                 `json:"protocol_address"`
	Chain           *string                 `json:"chain"`
	ClosingDate     *int                    `json:"closing_date"`
	Nft             *OpenseaNftAsset        `json:"nft"`
	Quantity        *int                    `json:"quantity"`
	Seller          *string                 `json:"seller"`
	Buyer           *string                 `json:"buyer"`
	Payment         *OpenseaNftEventPayment `json:"payment"`
	FromAddress     *string                 `json:"from_address"`
	ToAddress       *string                 `json:"to_address"`
	Transaction     *string                 `json:"transaction"`
	EventTimestamp  *int                    `json:"event_timestamp"`
}

type OpenSeaListResponse struct {
	Nfts   []OpenseaNftAsset `json:"nfts"`
	Events []OpenseaNftEvent `json:"asset_events"`
	Next   *string           `json:"next"`
}

const OpenSeaListLimit = 200

func GetListOpenseaData(url string) (*OpenSeaListResponse, error) {
	req, err := http.NewRequest("GET", url, nil)
	if err != nil {
		return nil, err
	}

	req.Header.Add("accept", "application/json")
	req.Header.Add("x-api-key", os.Getenv("OPENSEA_API_KEY"))

	res, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, err
	}

	defer func(Body io.ReadCloser) {
		_ = Body.Close()
	}(res.Body)
	body, err := io.ReadAll(res.Body)
	if err != nil {
		return nil, err
	}

	var response = &OpenSeaListResponse{}
	err = json.Unmarshal(body, response)
	if err != nil {
		return nil, err
	}

	return response, nil
}
