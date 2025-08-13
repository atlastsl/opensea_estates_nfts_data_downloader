package operations

//func calculateDistance2Tiles(X1, Y1 int, XY2 string) (float64, int) {
//	_XY2 := strings.Split(XY2, ",")
//	t2X, _ := strconv.Atoi(_XY2[0])
//	t2Y, _ := strconv.Atoi(_XY2[1])
//	return helpers.EuclidDistance(X1, Y1, t2X, t2Y), helpers.ManhattanDistance(X1, Y1, t2X, t2Y)
//}
//
//func calculateDistanceToFocalZone(X, Y int, focalZone *MapFocalZone) *MapFocalZoneDistance {
//	minEucD := math.MaxFloat64
//	minManD := math.MaxInt
//	for _, parcel := range focalZone.Parcels {
//		euc_d, man_d := calculateDistance2Tiles(X, Y, parcel)
//		if euc_d < minEucD {
//			minEucD = euc_d
//		}
//		if man_d < minManD {
//			minManD = man_d
//		}
//	}
//	return &MapFocalZoneDistance{X: X, Y: Y, FocalZone: focalZone, EucDis: minEucD, ManDis: minManD}
//}
//
//func calculateDistanceToFocalZone2(XY string, focalZone *MapFocalZone) *MapFocalZoneDistance {
//	_XY := strings.Split(XY, ",")
//	X, _ := strconv.Atoi(_XY[0])
//	Y, _ := strconv.Atoi(_XY[1])
//	return calculateDistanceToFocalZone(X, Y, focalZone)
//}
