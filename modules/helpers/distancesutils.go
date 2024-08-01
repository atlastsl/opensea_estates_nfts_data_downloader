package helpers

import "math"

func EuclidDistance(x1, y1, x2, y2 int) float64 {
	return math.Pow(math.Pow(float64(x1)-float64(x2), 2)+math.Pow(float64(y1)-float64(y2), 2), 0.5)
}

func ManhattanDistance(x1, y1, x2, y2 int) int {
	return int(math.Abs(float64(x1-x2)) + math.Abs(float64(y1-y2)))
}
