package tiles

import (
	"decentraland_data_downloader/modules/helpers"
	"encoding/json"
	"os"
)

func getDclDistrictData() ([]DclMapDistrict, error) {
	//urlDistricts := "https://api.decentraland.org/v2/districts"
	//jsonResDistrict, err := helpers.FetchData(urlDistricts, "")
	//if err != nil {
	//	return nil, err
	//}
	//resDistricts := DclMapDistrictRes{}
	//err = helpers.ConvertMapToStruct(jsonResDistrict, &resDistricts)
	//if err != nil {
	//	return nil, err
	//}
	//return resDistricts.Data, nil
	jsonResDistrictStr, err := os.ReadFile("./files/decentraland/districts.json")
	if err != nil {
		return nil, err
	}
	var jsonResDistrict map[string]any
	err = json.Unmarshal(jsonResDistrictStr, &jsonResDistrict)
	if err != nil {
		return nil, err
	}
	resDistricts := DclMapDistrictRes{}
	err = helpers.ConvertMapToStruct(jsonResDistrict, &resDistricts)
	if err != nil {
		return nil, err
	}
	return resDistricts.Data, nil
}

func getDclTilesData() (map[string]DclMapTile, error) {
	urlTiles := "https://api.decentraland.org/v2/tiles?include=id,x,y,type,estateId,name"
	//urlTiles := "https://api.decentraland.org/v2/tiles?include=id,x,y,type,estateId,name&x1=10&y1=10&x2=50&y2=50"
	jsonResTiles, err := helpers.FetchData(urlTiles, "")
	if err != nil {
		return nil, err
	}
	resTiles := DclMapTilesRes{}
	err = helpers.ConvertMapToStruct(jsonResTiles, &resTiles)
	if err != nil {
		return nil, err
	}
	return resTiles.Data, nil
}
