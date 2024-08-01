package helpers

import (
	"github.com/ethereum/go-ethereum/common/hexutil"
	"strings"
)

func HexRemoveLeadingZeros(s string) string {
	if strings.HasPrefix(s, "0x") {
		return "0x" + strings.TrimLeft(s[2:], "0")
	}
	return strings.TrimLeft(s, "0")
}

func HexConvertToInt(s string) (int, error) {
	cleanHex := HexRemoveLeadingZeros(s)
	bs, err := hexutil.DecodeUint64(cleanHex)
	if err != nil {
		return 0, err
	}
	return int(bs), nil
}

func HexConvertToString(s string) (string, error) {
	cleanHex := HexRemoveLeadingZeros(s)
	bs, err := hexutil.DecodeBig(cleanHex)
	if err != nil {
		return "", err
	}
	return bs.String(), nil
}
