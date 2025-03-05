package helpers

import "strings"

func CodeFlattenString(str string) string {
	parts := strings.Split(str, " ")
	parts = ArrayMap(parts, func(t string) (bool, string) {
		return true, strings.ToLower(t)
	}, true, "")
	return strings.Join(parts, "_")
}

func CapitalizeFirstLetter(s string) string {
	return strings.ToUpper(s[0:1]) + s[1:]
}
