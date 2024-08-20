package blocks_info

import (
	"decentraland_data_downloader/modules/core/collections"
	"fmt"
	"go.mongodb.org/mongo-driver/mongo"
	"math"
)

const PartitionsNbItem = 7

func getBlockNumbers(collection collections.Collection, dbInstance *mongo.Database) (map[string][]uint64, error) {
	distinctBlocks, err := getDistinctBlockNumbersFromDatabase(collection, dbInstance)
	if err != nil {
		return nil, err
	}
	nbParts := int(math.Ceil(float64(len(distinctBlocks)) / float64(PartitionsNbItem)))
	blockNumbers := make(map[string][]uint64)
	for i := 0; i < nbParts; i++ {
		start := i * PartitionsNbItem
		end := start + PartitionsNbItem
		if end > len(distinctBlocks) {
			end = len(distinctBlocks)
		}
		bn1, bn2 := distinctBlocks[start], distinctBlocks[end-1]
		key := fmt.Sprintf("%d_%d", bn1, bn2)
		blockNumbers[key] = distinctBlocks[start:end]
	}
	return blockNumbers, nil
}
