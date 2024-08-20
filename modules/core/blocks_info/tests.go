package blocks_info

import (
	"encoding/json"
	"fmt"
)

func TestBlocksInfo() {
	blockNumbers := []uint64{
		19500000,
		19600000,
	}
	data, err := fetchBlocksTimestamps(blockNumbers)
	if err != nil {
		panic(err)
	}
	str, _ := json.MarshalIndent(data, "", "  ")
	fmt.Println(string(str))
}
