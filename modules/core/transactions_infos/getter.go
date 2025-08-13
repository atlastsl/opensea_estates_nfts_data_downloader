package transactions_infos

import (
	"decentraland_data_downloader/modules/core/metaverses"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
)

const PartitionsNbItem = 100

func getTransactionsHashesSlices(metaverse metaverses.MetaverseName, dbInstance *mongo.Database) (map[string][]*transactionInput, error) {
	allTransactionsHashes, err := getTransactionHashesFromDatabase(metaverse, dbInstance)
	if err != nil {
		return nil, err
	}
	txHashesSlices := make(map[string][]*transactionInput)
	for blockchain, transactionsHashes := range allTransactionsHashes {
		nbParts := int(math.Ceil(float64(len(transactionsHashes)) / float64(PartitionsNbItem)))
		for i := 0; i < nbParts; i++ {
			start := i * PartitionsNbItem
			end := start + PartitionsNbItem
			if end > len(transactionsHashes) {
				end = len(transactionsHashes)
			}
			txHash1 := transactionsHashes[start]
			key := fmt.Sprintf("%s_%s_%d", blockchain, txHash1.txHash.TransactionHash, txHash1.txHash.BlockTimestamp.UnixMilli())
			txHashesSlices[key] = transactionsHashes[start:end]
		}
	}
	return txHashesSlices, nil
}
