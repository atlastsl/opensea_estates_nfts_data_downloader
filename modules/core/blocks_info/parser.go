package blocks_info

import (
	"cloud.google.com/go/bigquery"
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/metaverses"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"fmt"
	"github.com/ethereum/go-ethereum/common/hexutil"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"os"
	"slices"
	"sync"
	"time"
)

func fetchBlocksTimestampsFromBQDatabase(blockNumbers []uint64, blockchain string) ([]*helpers.EthBlockInfo, error) {
	projectId := os.Getenv("ETHEREUM_ETL_PROJECT_ID")
	credsFile := os.Getenv("BIG_QUERY_CREDENTIALS_FILE")
	client, err := bigquery.NewClient(context.Background(), projectId, option.WithCredentialsFile(credsFile))
	if err != nil {
		return nil, err
	}
	queryStr := fmt.Sprintf(
		`
		SELECT number as block_number, timestamp as block_timestamp
		FROM bigquery-public-data.crypto_%s.blocks
		WHERE number IN UNNEST(@block_numbers)
		ORDER BY block_number ASC
	`, blockchain)
	query := client.Query(queryStr)
	blockNumbersInt64 := make([]int64, len(blockNumbers))
	for i, v := range blockNumbers {
		blockNumbersInt64[i] = int64(v)
	}

	query.Parameters = []bigquery.QueryParameter{
		{Name: "block_numbers", Value: blockNumbersInt64},
	}
	it, err := query.Read(context.Background())
	if err != nil {
		return nil, err
	}
	blockInfos := make([]*helpers.EthBlockInfo, 0)
	for {
		blockInfo := &helpers.EthBlockInfo{}
		err = it.Next(blockInfo)
		if errors.Is(err, iterator.Done) {
			break
		}
		if err != nil {
			return nil, err
		}
		blockInfos = append(blockInfos, blockInfo)
	}
	return blockInfos, nil
}

func fetchBlockInfoFromInfura(blockNumber uint64, blockchain string) (*helpers.EthBlockInfo, error) {
	payloadMap := map[string]any{
		"jsonrpc": "2.0",
		"method":  "eth_getBlockByNumber",
		"id":      time.Now().UnixMilli(),
		"params":  []any{hexutil.EncodeUint64(blockNumber), false},
	}
	blockInfo := map[string]any{}
	err := helpers.InfuraRequest(blockchain, payloadMap, &blockInfo)
	if err != nil {
		return nil, err
	}
	timestampHex, ok := blockInfo["timestamp"]
	if !ok {
		return nil, nil
	}
	timestampHexStr := timestampHex.(string)
	timestamp, _ := hexutil.DecodeUint64(timestampHexStr)
	return &helpers.EthBlockInfo{BlockNumber: int64(blockNumber), Blockchain: blockchain, BlockTimestamp: time.UnixMilli(int64(timestamp * 1000))}, nil
}

func fetchBlocksTimestampsFromInfura(blockNumbers []uint64, blockchain string) ([]*helpers.EthBlockInfo, error) {
	blockInfos := make([]*helpers.EthBlockInfo, 0)
	for _, blockNumber := range blockNumbers {
		blockInfo, err := fetchBlockInfoFromInfura(blockNumber, blockchain)
		if err != nil {
			return nil, err
		}
		if blockInfo != nil {
			blockInfos = append(blockInfos, blockInfo)
		}
		time.Sleep(500 * time.Millisecond)
	}
	return blockInfos, nil
}

func fetchBlocksTimestamps(blockNumbers []uint64, blockchain string) ([]*helpers.EthBlockInfo, error) {
	bqDbBlockInfos, err := fetchBlocksTimestampsFromBQDatabase(blockNumbers, blockchain)
	if err != nil {
		return nil, err
	}
	foundBlockNumbers := helpers.ArrayMap(bqDbBlockInfos, func(t *helpers.EthBlockInfo) (bool, uint64) {
		return true, uint64(t.BlockNumber)
	}, true, 0)
	notFoundBlockNumbers := helpers.ArrayFilter(blockNumbers, func(u uint64) bool {
		return !slices.Contains(foundBlockNumbers, u)
	})
	if len(notFoundBlockNumbers) > 0 {
		infBlockInfos, e2 := fetchBlocksTimestampsFromInfura(blockNumbers, blockchain)
		if e2 != nil {
			return nil, e2
		}
		bqDbBlockInfos = append(bqDbBlockInfos, infBlockInfos...)
	}
	return bqDbBlockInfos, nil
}

func saveBlockTimestamps(blockInfos []*helpers.EthBlockInfo, metaverse metaverses.MetaverseName) error {
	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	err = saveBlockTimestampInDatabase(blockInfos, metaverse, dbInstance)
	return err
}

func parseBlockTimestamps(blockNumbers []uint64, blockchain string, metaverse metaverses.MetaverseName, wg *sync.WaitGroup) error {
	blockInfos, err := fetchBlocksTimestamps(blockNumbers, blockchain)
	if err != nil {
		return err
	}

	dbInstance, err := database.NewDatabaseConnection()
	if err != nil {
		return err
	}
	defer database.CloseDatabaseConnection(dbInstance)

	wg.Add(1)
	go func() {
		defer wg.Done()
		_ = saveBlockTimestamps(blockInfos, metaverse)
	}()

	return nil
}
