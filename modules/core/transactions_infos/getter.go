package transactions_infos

import (
	"decentraland_data_downloader/modules/core/collections"
	"decentraland_data_downloader/modules/core/transactions_hashes"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
)

const PartitionsNbItem = 100

func getTransactionsHashesSlices(collection collections.Collection, dbInstance *mongo.Database) (map[string][]*transactions_hashes.TransactionHash, error) {
	transactionsHashes, err := getTransactionHashesFromDatabase(collection, dbInstance)
	if err != nil {
		return nil, err
	}
	nbParts := int(math.Ceil(float64(len(transactionsHashes)) / float64(PartitionsNbItem)))
	txHashesSlices := make(map[string][]*transactions_hashes.TransactionHash)
	for i := 0; i < nbParts; i++ {
		start := i * PartitionsNbItem
		end := start + PartitionsNbItem
		if end > len(transactionsHashes) {
			end = len(transactionsHashes)
		}
		txHash1 := transactionsHashes[start]
		key := fmt.Sprintf("%s_%d", txHash1.TransactionHash, txHash1.BlockTimestamp.UnixMilli())
		txHashesSlices[key] = transactionsHashes[start:end]
	}
	return txHashesSlices, nil
}
