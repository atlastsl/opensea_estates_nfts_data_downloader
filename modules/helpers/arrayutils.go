package helpers

import (
	"golang.org/x/exp/slices"
	"math/rand"
	"time"
)

func ArrayFilter[T any](arr []T, filter func(T) bool) (ret []T) {
	for _, ss := range arr {
		if filter(ss) {
			ret = append(ret, ss)
		}
	}
	return
}

func ArrayMap[T, U any](arr []T, mapper func(T) (bool, U), skipNotExists bool, defaultValue U) (ret []U) {
	for _, ss := range arr {
		ok, u := mapper(ss)
		if ok {
			ret = append(ret, u)
		} else if !skipNotExists {
			ret = append(ret, defaultValue)
		}
	}
	return
}

func ArrayAppend[T comparable](base, arr []T) []T {
	for _, ss := range arr {
		if !slices.Contains(base, ss) {
			base = append(base, ss)
		}
	}
	return base
}

func MapGetKeys[T comparable, U any](m map[T]U) []T {
	keys := make([]T, 0, len(m))
	for k := range m {
		keys = append(keys, k)
	}
	return keys
}

func FloatArrayAvg(arr []float64, defaultValue float64) (avg float64) {
	avg = defaultValue
	if len(arr) > 0 {
		avg = 0
		for _, tt := range arr {
			avg += tt
		}
		avg = avg / float64(len(arr))
	}
	return
}

func ArrayCopy[T any](source []T) []T {
	var cpy []T
	if len(source) > 0 {
		cpy = make([]T, len(source))
		copy(cpy, source)
	}
	return cpy
}

func SlicePickNRandom[T any](slice []T, n int) []T {
	if n > len(slice) {
		n = len(slice) // Cannot pick more elements than available
	}
	r := rand.New(rand.NewSource(time.Now().UnixNano()))
	shuffled := make([]T, len(slice))
	copy(shuffled, slice) // Create a copy to avoid modifying the original
	r.Shuffle(len(shuffled), func(i, j int) {
		shuffled[i], shuffled[j] = shuffled[j], shuffled[i]
	})
	return shuffled[:n]
}
