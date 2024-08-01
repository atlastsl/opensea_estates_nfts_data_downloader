package helpers

import (
	"math"
	"math/big"
)

func ConvertBigAmountToFloat64(amount string, decimals int) (float64, error) {
	n := new(big.Float)
	n.SetString(amount)
	d := new(big.Float)
	d.SetFloat64(math.Pow(10.0, float64(decimals)))
	r := new(big.Float)
	r.Quo(n, d)
	f, _ := n.Float64()
	return f, nil
}
