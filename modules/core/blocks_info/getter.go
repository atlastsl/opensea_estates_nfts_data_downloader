package blocks_info

import (
	"decentraland_data_downloader/modules/core/collections"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
)

const PartitionsNbItem = 1000

func getBlockNumbers(collection collections.Collection, dbInstance *mongo.Database) (map[string][]uint64, error) {
	allDistinctBlocks, err := getDistinctBlockNumbersFromDatabase(collection, dbInstance)
	if err != nil {
		return nil, err
	}
	blockNumbers := make(map[string][]uint64)
	for blockchain, distinctBlocks := range allDistinctBlocks {
		nbParts := int(math.Ceil(float64(len(distinctBlocks)) / float64(PartitionsNbItem)))
		for i := 0; i < nbParts; i++ {
			start := i * PartitionsNbItem
			end := start + PartitionsNbItem
			if end > len(distinctBlocks) {
				end = len(distinctBlocks)
			}
			bn1, bn2 := distinctBlocks[start], distinctBlocks[end-1]
			key := fmt.Sprintf("%s_%d_%d", blockchain, bn1, bn2)
			blockNumbers[key] = distinctBlocks[start:end]
		}
	}
	return blockNumbers, nil
}
