package operations

var plazaListNames map[string]string = map[string]string{
	"1092": "North Genesis Plaza",
	"1094": "North-East Genesis Plaza",
	"1134": "North-West Genesis Plaza",
	"1096": "East Genesis Plaza",
	"1132": "West Genesis Plaza",
	"1164": "Central Genesis Plaza",
	"1112": "South-East Genesis Plaza",
	"1130": "South-West Genesis Plaza",
	"1127": "South Genesis Plaza",
	"2841": "Mini Plaza",
	"2840": "Mini Plaza",
}

//func getDclDistrictData() ([]DclMapDistrict, error) {
//	jsonResDistrictStr, err := os.ReadFile("./files/decentraland/districts.json")
//	if err != nil {
//		return nil, err
//	}
//	var jsonResDistrict map[string]any
//	err = json.Unmarshal(jsonResDistrictStr, &jsonResDistrict)
//	if err != nil {
//		return nil, err
//	}
//	resDistricts := DclMapDistrictRes{}
//	err = helpers.ConvertMapToStruct(jsonResDistrict, &resDistricts)
//	if err != nil {
//		return nil, err
//	}
//	return resDistricts.Data, nil
//}
//
//func getDclTilesData() (map[string]DclMapTile, error) {
//	urlTiles := "https://api.decentraland.org/v2/tiles?include=id,x,y,type,estateId,name"
//	//urlTiles := "https://api.decentraland.org/v2/tiles?include=id,x,y,type,estateId,name&x1=10&y1=10&x2=50&y2=50"
//	jsonResTiles, err := helpers.FetchData(urlTiles, "")
//	if err != nil {
//		return nil, err
//	}
//	resTiles := DclMapTilesRes{}
//	err = helpers.ConvertMapToStruct(jsonResTiles, &resTiles)
//	if err != nil {
//		return nil, err
//	}
//	return resTiles.Data, nil
//}
//
//func parseDclTileInfo(tileId string, dclTile DclMapTile, districts []DclMapDistrict, collectionName string) (*MapFocalZone, string) {
//	insideType, insideSubType, insideName, insideId := "nothing", "", "", ""
//	if dclTile.Type == "plaza" || dclTile.Type == "road" {
//		insideType = dclTile.Type
//		insideName = dclTile.Name
//		insideId = dclTile.EstateId
//		if dclTile.Type == "plaza" {
//			tmp, ok := plazaListNames[insideId]
//			if ok {
//				insideSubType = tmp
//			} else {
//				insideSubType = "Unknown Plaza"
//			}
//		} else {
//			insideSubType = "Road"
//		}
//	} else if dclTile.Type == "district" {
//		idx := slices.IndexFunc(districts, func(district DclMapDistrict) bool {
//			return slices.Contains(district.Parcels, tileId)
//		})
//		insideType = dclTile.Type
//		if idx >= 0 {
//			insideName = districts[idx].Name
//			insideId = fmt.Sprintf("Dst-%d", idx)
//			insideSubType = fmt.Sprintf("District [%s]", districts[idx].Category)
//		} else {
//			insideType = "nothing"
//			insideSubType = "Nothing"
//		}
//	}
//	var focalZone = MapFocalZone{
//		Collection: collectionName,
//		Contract:   os.Getenv("DECENTRALAND_LAND_CONTRACT"),
//		Type:       insideType,
//		Subtype:    insideSubType,
//		Slug:       fmt.Sprintf("%s-%s", strings.ReplaceAll(insideName, " ", "-"), insideId),
//		Name:       insideName,
//		FZoneId:    insideId,
//	}
//	return &focalZone, dclTile.Coords
//}

//func dclPrepareFocalZones(collection metaverses.Collection) ([]*MapFocalZone, error) {
//	rawLandsData, err := getDclTilesData()
//	if err != nil {
//		return nil, err
//	}
//	districtsData, err := getDclDistrictData()
//	if err != nil {
//		return nil, err
//	}
//	focalZones := make([]*MapFocalZone, 0)
//	for tileId, tile := range rawLandsData {
//		focalZone, parcel := parseDclTileInfo(tileId, tile, districtsData, string(collection))
//		if focalZone.Type != "nothing" {
//			idx := slices.IndexFunc(focalZones, func(fz *MapFocalZone) bool {
//				return fz.Type == focalZone.Type && fz.Subtype == focalZone.Subtype
//			})
//			if idx < 0 {
//				focalZones = append(focalZones, focalZone)
//				idx = len(focalZones) - 1
//			}
//			focalZones[idx].Parcels = append(focalZones[idx].Parcels, parcel)
//		}
//	}
//	return focalZones, nil
//}
