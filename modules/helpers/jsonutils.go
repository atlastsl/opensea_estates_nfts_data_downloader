package helpers

import (
	"encoding/json"
	"fmt"
)

func PrettyPrintObject(object interface{}) {
	jsonStr, err := json.MarshalIndent(object, "", "  ")
	if err != nil {
		panic(err)
	}
	fmt.Println(string(jsonStr))
}
