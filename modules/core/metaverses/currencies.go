package metaverses

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/helpers"
	"fmt"
	"github.com/gocarina/gocsv"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"os"
	"path/filepath"
	"time"
)

var currencies = []Currency{
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x0000000000000000000000000000000000000000",
	//	Decimals:     18,
	//	Symbols:      "ETH",
	//	Name:         "Ether",
	//	PriceSlug:    "ethereum",
	//	MainCurrency: true,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
	//	Decimals:     18,
	//	Symbols:      "WETH",
	//	Name:         "Wrapped ETH",
	//	PriceSlug:    "ethereum",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2",
	//	Decimals:     18,
	//	Symbols:      "MKR",
	//	Name:         "Maker",
	//	PriceSlug:    "maker",
	//	MainCurrency: false,
	//},
	{
		Blockchain:   EthereumBlockchain,
		Contract:     "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Decimals:     6,
		Symbols:      "USDC",
		Name:         "USD Coin",
		PriceSlug:    "usd-coin",
		MainCurrency: false,
	},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359",
	//	Decimals:     18,
	//	Symbols:      "SAI",
	//	Name:         "Single-Collateral DAI",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
	//	Decimals:     18,
	//	Symbols:      "MANA",
	//	Name:         "Decentraland MANA",
	//	PriceSlug:    "decentraland",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xb8c77482e45f1f44de1745f52c74426c631bdd52",
	//	Decimals:     18,
	//	Symbols:      "BNB",
	//	Name:         "Binance Coin",
	//	PriceSlug:    "binance-coin",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x79d83B390cF0EDF86B9EFbE47B556Cc6e20926aC",
	//	Decimals:     18,
	//	Symbols:      "MANABNT",
	//	Name:         "MANABNT Smart Token Relay",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x6b175474e89094c44da98b954eedeac495271d0f",
	//	Decimals:     18,
	//	Symbols:      "DAI",
	//	Name:         "Multi-Collateral DAI",
	//	PriceSlug:    "dai",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c",
	//	Decimals:     18,
	//	Symbols:      "BNT",
	//	Name:         "Bancor Network Token",
	//	PriceSlug:    "bancor",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x1E0CD9506d465937E9d6754e76Cd389A8bD90FBf",
	//	Decimals:     18,
	//	Symbols:      "LAND20",
	//	Name:         "Decentraland LAND20 Token",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xf970b8e36e23f7fc3fd752eea86f8be8d83375a6",
	//	Decimals:     18,
	//	Symbols:      "RCN",
	//	Name:         "Ripio Credit Network Token",
	//	PriceSlug:    "ripio-credit-network",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x744d70fdbe2ba4cf95131626614a1763df805b9e",
	//	Decimals:     18,
	//	Symbols:      "SNT",
	//	Name:         "Status Network Token",
	//	PriceSlug:    "status",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xcd62b1c403fa761baadfc74c525ce2b51780b184",
	//	Decimals:     18,
	//	Symbols:      "ANJ",
	//	Name:         "Aragon Court",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
	//	Decimals:     8,
	//	Symbols:      "WBTC",
	//	Name:         "Wrapped BTC",
	//	PriceSlug:    "wrapped-bitcoin",
	//	MainCurrency: false,
	//},
	{
		Blockchain:   EthereumBlockchain,
		Contract:     "0xdAC17F958D2ee523a2206206994597C13D831ec7",
		Decimals:     6,
		Symbols:      "USDT",
		Name:         "Tether USD",
		PriceSlug:    "tether",
		MainCurrency: false,
	},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x27fd686db10e0ae047fe8fe1de9830c0e0dc3cfa",
	//	Decimals:     4,
	//	Symbols:      "SCOTT",
	//	Name:         "SCOTT",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xc0bfeba72805f22dc18dde31467c5a55c16ff57b",
	//	Decimals:     18,
	//	Symbols:      "META",
	//	Name:         "META Dsc",
	//	PriceSlug:    "meta-bsc",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x92f5ee3bedb444fd079c288fe3a890ad6de28ecb",
	//	Decimals:     18,
	//	Symbols:      "APS",
	//	Name:         "Afterparty Shards",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xfca59cd816ab1ead66534d82bc21e7515ce441cf",
	//	Decimals:     18,
	//	Symbols:      "RARI",
	//	Name:         "Rarible Token",
	//	PriceSlug:    "rarible",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xd6c5934dfc75ead4f095be83091eb12e455175fd",
	//	Decimals:     18,
	//	Symbols:      "DPTS",
	//	Name:         "Decentraland Parcel Test",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x2c689b7b0f1cd7482450cdf72fcb63fca1693e66",
	//	Decimals:     18,
	//	Symbols:      "EGCS",
	//	Name:         "East Genesis Corner Shards",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x7ccdc136619cddf744122a938b4448eda1590fe1",
	//	Decimals:     0,
	//	Symbols:      "CREATION",
	//	Name:         "The Creation",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x81e8cdcc1914343d2cd1dfa50f83dc2306e04888",
	//	Decimals:     18,
	//	Symbols:      "VCS",
	//	Name:         "Vice City Shards",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x030bA81f1c18d280636F32af80b9AAd02Cf0854e",
	//	Decimals:     18,
	//	Symbols:      "aWETH",
	//	Name:         "Aave interest bearing WETH",
	//	PriceMap:     "WETH",
	//	PriceSlug:    "ethereum",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x98a0f212E313767a2Cb3084EC6bCf939106c50C2",
	//	Decimals:     18,
	//	Symbols:      "EGCS2",
	//	Name:         "East Genesis Corner Shards 2",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xf581fa0a5f6909741d75f1610106144cd157925e",
	//	Decimals:     18,
	//	Symbols:      "APS2",
	//	Name:         "Afterparty Shards 2",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xD607376d92Adfe49CFDD96A7B553CB6586A67a43",
	//	Decimals:     18,
	//	Symbols:      "FSE",
	//	Name:         "First Shards Ever",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x7f649eAb3C4C244b6F379843F41849182b14D7a7",
	//	Decimals:     18,
	//	Symbols:      "UNI-V1",
	//	Name:         "Uniswap V1",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x0C83d52140fF0FDB1a46FBE7a7f15216cbe70896",
	//	Decimals:     18,
	//	Symbols:      "SST",
	//	Name:         "Second Shards Test",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x1cCbAB3263099d9787810C3F5d55db7a8710FE2F",
	//	Decimals:     18,
	//	Symbols:      "DLAND",
	//	Name:         "Decentraland Land DLAND",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0x3845badade8e6dff049820680d1f14bd3903a5d0",
	//	Decimals:     18,
	//	Symbols:      "SAND",
	//	Name:         "The Sandbox SAND",
	//	PriceSlug:    "the-sandbox",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xb834B64B1462AFE8F30E83f0005C17346Ca9C567",
	//	Decimals:     18,
	//	Symbols:      "UNI-V1-2",
	//	Name:         "Uniswap V1 N2",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xBcca60bB61934080951369a648Fb03DF4F96263C",
	//	Decimals:     6,
	//	Symbols:      "aUSDC",
	//	Name:         "Aave interest bearing USDC",
	//	PriceMap:     "USDC",
	//	PriceSlug:    "usd-coin",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xE36804028b9DA57D8979C60721bAB1Eeb5488114",
	//	Decimals:     18,
	//	Symbols:      "LP",
	//	Name:         "Decentraland Bounty",
	//	PriceSlug:    "",
	//	MainCurrency: false,
	//},
	//{
	//	Blockchain:   EthereumBlockchain,
	//	Contract:     "0xdf801468a808a32656d2ed2d2d80b72a129739f4",
	//	Decimals:     18,
	//	Symbols:      "CUBE",
	//	Name:         "Somnium Space Cube",
	//	PriceSlug:    "somnium-space-cubes",
	//	MainCurrency: false,
	//},
}

func fetchPrice(url string, result any, maxRetries int) error {
	rtCount := 0
	var err error
	for rtCount < maxRetries {
		rtCount++
		err = helpers.FetchData(url, "", &result)
		if err == nil {
			return nil
		} else {
			log.Println(fmt.Sprintf("Error fetching price from url %s (try #%d): %s", url, rtCount, err.Error()))
			time.Sleep(2 * time.Second)
		}
	}
	return err
}

func getCurrencyPrices(currency *Currency) []*CurrencyPrice {
	prices := make([]*CurrencyPrice, 0)

	if currency.PriceSlug == "" {
		log.Println("Currency has no prices registered " + currency.Name)
		return prices
	}

	pricesFilePath := filepath.Join("files", "prices", fmt.Sprintf("%s.csv", currency.PriceSlug))
	if _, err := os.Stat(pricesFilePath); os.IsNotExist(err) {
		log.Println("Currency prices file does not exist: " + pricesFilePath)
		return prices
	}

	pricesFile, err := os.OpenFile(pricesFilePath, os.O_RDONLY, os.ModePerm)
	if err != nil {
		log.Println("Currency prices file not readable: " + pricesFilePath)
		return prices
	}

	rawCurrencyPrices := make([]*CurrencyHPrice, 0)
	err = gocsv.UnmarshalFile(pricesFile, &rawCurrencyPrices)
	if err != nil {
		log.Println("Currency prices file failed to be parsed: " + pricesFilePath)
		return prices
	}

	for _, rawCurrencyPrice := range rawCurrencyPrices {
		start, _ := time.Parse(time.DateOnly, rawCurrencyPrice.StartDate)
		end, _ := time.Parse(time.DateOnly, rawCurrencyPrice.EndDate)
		pOpen := rawCurrencyPrice.Open
		pClose := rawCurrencyPrice.Close
		pHigh := rawCurrencyPrice.High
		pLow := rawCurrencyPrice.Low
		pAvg := (rawCurrencyPrice.Open + rawCurrencyPrice.Close + rawCurrencyPrice.High + rawCurrencyPrice.Low) / 4
		volume := rawCurrencyPrice.Volume
		marketCap := rawCurrencyPrice.MarketCap
		price := &CurrencyPrice{Currency: currency.Symbols, Start: start, End: end, Open: pOpen, Close: pClose, High: pHigh, Low: pLow, Avg: pAvg, Volume: volume, MarketCap: marketCap}
		prices = append(prices, price)
	}

	return prices
}

func downloadCurrencyPrices(currency *Currency) []*CurrencyPrice {
	prices := make([]*CurrencyPrice, 0)

	if currency.PriceSlug == "" {
		log.Println("Currency has no prices registered " + currency.Name)
		return prices
	}

	dateEnd := time.Now().Format(time.DateOnly)
	baseUrl := fmt.Sprintf("https://coincodex.com/api/coincodexcoins/get_historical_data_by_slug/%s/2005-01-01/%s/1?t=5850862", currency.PriceSlug, dateEnd)
	result := make(map[string]any)
	//err := helpers.FetchData(baseUrl, "", &result)
	err := fetchPrice(baseUrl, &result, 5)
	if err != nil {
		log.Fatal("Unable to download prices for currency "+currency.Name, err)
	}
	records := result["data"].([]any)

	for _, _record := range records {
		record := _record.(map[string]interface{})
		start, _ := time.Parse(time.DateTime, record["time_start"].(string))
		end, _ := time.Parse(time.DateTime, record["time_end"].(string))
		pOpen := record["price_open_usd"].(float64)
		pClose := record["price_close_usd"].(float64)
		pHigh := record["price_high_usd"].(float64)
		pLow := record["price_low_usd"].(float64)
		pAvg := record["price_avg_usd"].(float64)
		volume := record["volume_usd"].(float64)
		marketCap := record["market_cap_usd"].(float64)
		price := &CurrencyPrice{Currency: currency.Symbols, Start: start, End: end, Open: pOpen, Close: pClose, High: pHigh, Low: pLow, Avg: pAvg, Volume: volume, MarketCap: marketCap}
		prices = append(prices, price)
	}

	return prices
}

func SaveCurrencies() {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		panic(err)
	}
	operations := make([]mongo.WriteModel, len(currencies))
	pricesOperations := make([]mongo.WriteModel, 0)
	for i, currency := range currencies {
		currency.CreatedAt = time.Now().UTC()
		currency.UpdatedAt = time.Now().UTC()
		payload := bson.M{"blockchain": currencies[i].Blockchain, "contract": currencies[i].Contract}
		operations[i] = mongo.NewReplaceOneModel().SetFilter(payload).SetReplacement(&currency).SetUpsert(true)
		prices := getCurrencyPrices(&currency)
		for _, price := range prices {
			prcPayload := bson.M{"currency": price.Currency, "start": price.Start, "end": price.End}
			prcOperation := mongo.NewReplaceOneModel().SetFilter(prcPayload).SetReplacement(price).SetUpsert(true)
			pricesOperations = append(pricesOperations, prcOperation)
		}
	}

	curDbCollection := database.CollectionInstance(dbInstance, &Currency{})
	_, err = curDbCollection.BulkWrite(context.Background(), operations)
	if err != nil {
		panic(err)
	}
	prcDbCollection := database.CollectionInstance(dbInstance, &CurrencyPrice{})
	_, err = prcDbCollection.BulkWrite(context.Background(), pricesOperations)
	if err != nil {
		panic(err)
	}
}
