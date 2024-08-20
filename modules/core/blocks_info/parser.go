package blocks_info

import (
	"cloud.google.com/go/bigquery"
	"context"
	"decentraland_data_downloader/modules/app/database"
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/helpers"
	"errors"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
	"os"
	"sync"
)

func fetchBlocksTimestamps(blockNumbers []uint64) ([]*helpers.EthBlockInfo, error) {
	projectId := os.Getenv("ETHEREUM_ETL_PROJECT_ID")
	credsFile := os.Getenv("BIG_QUERY_CREDENTIALS_FILE")
	client, err := bigquery.NewClient(context.Background(), projectId, option.WithCredentialsFile(credsFile))
	if err != nil {
		return nil, err
	}
	query := client.Query(`
		SELECT number as block_number, timestamp as block_timestamp
		FROM bigquery-public-data.crypto_ethereum.blocks
		WHERE number IN UNNEST(@block_numbers)
		ORDER BY block_number ASC
	`)
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

func saveBlockTimestamps(blockNumbers []uint64, collection collections.Collection, wg *sync.WaitGroup) error {
	blockInfos, err := fetchBlocksTimestamps(blockNumbers)
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
		_ = saveBlockTimestampInDatabase(blockInfos, collection, dbInstance)
	}()

	return nil
}
