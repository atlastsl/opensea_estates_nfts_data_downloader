package collections

import (
	"github.com/kamva/mgm/v3"
	"slices"
	"time"
)

type CollectionInfoAsset struct {
	Blockchain string `bson:"blockchain,omitempty"`
	Name       string `bson:"name,omitempty"`
	Contract   string `bson:"contract,omitempty"`
}

type CollectionInfoLogTopic struct {
	Blockchain string   `bson:"blockchain,omitempty"`
	Name       string   `bson:"name,omitempty"`
	Hash       string   `bson:"hash,omitempty"`
	Contracts  []string `bson:"contracts,omitempty"`
	StartBlock uint64   `bson:"start_block,omitempty"`
	EndBlock   uint64   `bson:"end_block,omitempty"`
}

type CollectionInfo struct {
	mgm.DefaultModel `bson:",inline"`
	Name             string                   `bson:"name,omitempty"`
	Blockchain       []string                 `bson:"blockchain,omitempty"`
	Currency         string                   `bson:"currency,omitempty"`
	Assets           []CollectionInfoAsset    `bson:"assets,omitempty"`
	LogTopics        []CollectionInfoLogTopic `bson:"log_topics,omitempty"`
}

func (cltInfo *CollectionInfo) GetAsset(name string) *CollectionInfoAsset {
	if cltInfo == nil {
		return nil
	}
	for _, asset := range cltInfo.Assets {
		if asset.Name == name {
			return &asset
		}
	}
	return nil
}

func (cltInfo *CollectionInfo) HasAsset(address, blockchain string) bool {
	if cltInfo == nil {
		return false
	}
	for _, asset := range cltInfo.Assets {
		if asset.Blockchain == blockchain && asset.Contract == address {
			return true
		}
	}
	return false
}

func (cltInfo *CollectionInfo) GetLogTopic(address, blockchain string, eventHex string) *CollectionInfoLogTopic {
	if cltInfo == nil {
		return nil
	}
	for _, logTopic := range cltInfo.LogTopics {
		if logTopic.Blockchain == blockchain && slices.Contains(logTopic.Contracts, address) && logTopic.Hash == eventHex {
			return &logTopic
		}
	}
	return nil
}

type Currency struct {
	mgm.DefaultModel `bson:",inline"`
	Blockchain       string `bson:"blockchain,omitempty"`
	Contract         string `bson:"contract,omitempty"`
	Decimals         int64  `bson:"decimals,omitempty"`
	Name             string `bson:"name,omitempty"`
	Symbols          string `bson:"symbols,omitempty"`
	PriceMap         string `bson:"price_map,omitempty"`
	PriceSlug        string `bson:"price_slug,omitempty"`
}

type CurrencyPrice struct {
	mgm.DefaultModel `bson:",inline"`
	Currency         string    `bson:"currency,omitempty"`
	Start            time.Time `bson:"start,omitempty"`
	End              time.Time `bson:"end,omitempty"`
	Open             float64   `bson:"open,omitempty"`
	High             float64   `bson:"high,omitempty"`
	Low              float64   `bson:"low,omitempty"`
	Close            float64   `bson:"close,omitempty"`
	Avg              float64   `bson:"avg,omitempty"`
	Volume           float64   `bson:"volume,omitempty"`
	MarketCap        float64   `bson:"market_cap,omitempty"`
}
