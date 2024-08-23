package collections

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"encoding/csv"
	"fmt"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
	"log"
	"os"
	"strconv"
	"strings"
	"time"
)

var currencies = []Currency{
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x0000000000000000000000000000000000000000",
		Decimals:   18,
		Symbols:    "ETH",
		Name:       "Ether",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xc02aaa39b223fe8d0a0e5c4f27ead9083c756cc2",
		Decimals:   18,
		Symbols:    "WETH",
		Name:       "Wrapped ETH",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x9f8f72aa9304c8b593d555f12ef6589cc3a579a2",
		Decimals:   18,
		Symbols:    "MKR",
		Name:       "Maker",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xa0b86991c6218b36c1d19d4a2e9eb0ce3606eb48",
		Decimals:   6,
		Symbols:    "USDC",
		Name:       "USD Coin",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x89d24a6b4ccb1b6faa2625fe562bdd9a23260359",
		Decimals:   18,
		Symbols:    "SAI",
		Name:       "Single-Collateral DAI",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x0f5d2fb29fb7d3cfee444a200298f468908cc942",
		Decimals:   18,
		Symbols:    "MANA",
		Name:       "Decentraland MANA",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xb8c77482e45f1f44de1745f52c74426c631bdd52",
		Decimals:   18,
		Symbols:    "BNB",
		Name:       "Binance Coin",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x79d83B390cF0EDF86B9EFbE47B556Cc6e20926aC",
		Decimals:   18,
		Symbols:    "MANABNT",
		Name:       "MANABNT Smart Token Relay",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x6b175474e89094c44da98b954eedeac495271d0f",
		Decimals:   18,
		Symbols:    "DAI",
		Name:       "Multi-Collateral DAI",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x1f573d6fb3f13d689ff844b4ce37794d79a7ff1c",
		Decimals:   18,
		Symbols:    "BNT",
		Name:       "Bancor Network Token",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x1E0CD9506d465937E9d6754e76Cd389A8bD90FBf",
		Decimals:   18,
		Symbols:    "LAND20",
		Name:       "Decentraland LAND20 Token",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xf970b8e36e23f7fc3fd752eea86f8be8d83375a6",
		Decimals:   18,
		Symbols:    "RCN",
		Name:       "Ripio Credit Network Token",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x744d70fdbe2ba4cf95131626614a1763df805b9e",
		Decimals:   18,
		Symbols:    "SNT",
		Name:       "Status Network Token",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xcd62b1c403fa761baadfc74c525ce2b51780b184",
		Decimals:   18,
		Symbols:    "ANJ",
		Name:       "Aragon Court",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x2260FAC5E5542a773Aa44fBCfeDf7C193bc2C599",
		Decimals:   8,
		Symbols:    "WBTC",
		Name:       "Wrapped BTC",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xdAC17F958D2ee523a2206206994597C13D831ec7",
		Decimals:   6,
		Symbols:    "USDT",
		Name:       "Tether USD",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x27fd686db10e0ae047fe8fe1de9830c0e0dc3cfa",
		Decimals:   4,
		Symbols:    "SCOTT",
		Name:       "SCOTT",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xc0bfeba72805f22dc18dde31467c5a55c16ff57b",
		Decimals:   18,
		Symbols:    "META",
		Name:       "META Dsc",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x92f5ee3bedb444fd079c288fe3a890ad6de28ecb",
		Decimals:   18,
		Symbols:    "APS",
		Name:       "Afterparty Shards",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xfca59cd816ab1ead66534d82bc21e7515ce441cf",
		Decimals:   18,
		Symbols:    "RARI",
		Name:       "Rarible Token",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xd6c5934dfc75ead4f095be83091eb12e455175fd",
		Decimals:   18,
		Symbols:    "DPTS",
		Name:       "Decentraland Parcel Test",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x2c689b7b0f1cd7482450cdf72fcb63fca1693e66",
		Decimals:   18,
		Symbols:    "EGCS",
		Name:       "East Genesis Corner Shards",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x7ccdc136619cddf744122a938b4448eda1590fe1",
		Decimals:   0,
		Symbols:    "CREATION",
		Name:       "The Creation",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x81e8cdcc1914343d2cd1dfa50f83dc2306e04888",
		Decimals:   18,
		Symbols:    "VCS",
		Name:       "Vice City Shards",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x030bA81f1c18d280636F32af80b9AAd02Cf0854e",
		Decimals:   18,
		Symbols:    "aWETH",
		Name:       "Aave interest bearing WETH",
		PriceMap:   "WETH",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x98a0f212E313767a2Cb3084EC6bCf939106c50C2",
		Decimals:   18,
		Symbols:    "EGCS2",
		Name:       "East Genesis Corner Shards 2",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xf581fa0a5f6909741d75f1610106144cd157925e",
		Decimals:   18,
		Symbols:    "APS2",
		Name:       "Afterparty Shards 2",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xD607376d92Adfe49CFDD96A7B553CB6586A67a43",
		Decimals:   18,
		Symbols:    "FSE",
		Name:       "First Shards Ever",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x7f649eAb3C4C244b6F379843F41849182b14D7a7",
		Decimals:   18,
		Symbols:    "UNI-V1",
		Name:       "Uniswap V1",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x0C83d52140fF0FDB1a46FBE7a7f15216cbe70896",
		Decimals:   18,
		Symbols:    "SST",
		Name:       "Second Shards Test",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x1cCbAB3263099d9787810C3F5d55db7a8710FE2F",
		Decimals:   18,
		Symbols:    "DLAND",
		Name:       "Decentraland Land DLAND",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0x3845badade8e6dff049820680d1f14bd3903a5d0",
		Decimals:   18,
		Symbols:    "SAND",
		Name:       "The Sandbox SAND",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xb834B64B1462AFE8F30E83f0005C17346Ca9C567",
		Decimals:   18,
		Symbols:    "UNI-V1-2",
		Name:       "Uniswap V1 N2",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xBcca60bB61934080951369a648Fb03DF4F96263C",
		Decimals:   6,
		Symbols:    "aUSDC",
		Name:       "Aave interest bearing USDC",
		PriceMap:   "USDC",
	},
	{
		Blockchain: EthereumBlockchain,
		Contract:   "0xE36804028b9DA57D8979C60721bAB1Eeb5488114",
		Decimals:   18,
		Symbols:    "LP",
		Name:       "Decentraland Bounty",
	},
}

func readPriceCsv(currency *Currency) []*CurrencyPrice {
	prices := make([]*CurrencyPrice, 0)

	filePath := fmt.Sprintf("./files/prices/%s_prices.csv", strings.ToLower(currency.Symbols))
	f, err := os.Open(filePath)
	if err != nil {
		log.Println("Unable to read input file "+filePath, err)
		return prices
	}
	defer f.Close()

	csvReader := csv.NewReader(f)
	records, err := csvReader.ReadAll()
	if err != nil {
		log.Fatal("Unable to parse file as CSV for "+filePath, err)
	}

	for _, record := range records[1:] {
		start, _ := time.Parse(time.DateOnly, record[0])
		end, _ := time.Parse(time.DateOnly, record[1])
		pOpen, _ := strconv.ParseFloat(record[2], 64)
		pClose, _ := strconv.ParseFloat(record[3], 64)
		pHigh, _ := strconv.ParseFloat(record[4], 64)
		pLow, _ := strconv.ParseFloat(record[5], 64)
		volume, _ := strconv.ParseFloat(record[6], 64)
		marketCap, _ := strconv.ParseFloat(record[7], 64)
		price := &CurrencyPrice{Currency: currency.Symbols, Start: start, End: end, Open: pOpen, Close: pClose, High: pHigh, Low: pLow, Volume: volume, MarketCap: marketCap}
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
		prices := readPriceCsv(&currency)
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
