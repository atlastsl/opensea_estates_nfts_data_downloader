package tiles

import (
	"decentraland_data_downloader/modules/helpers"
	"reflect"
)

func TestGetDistrictsData() {
	data, err := getDclDistrictData()
	if err != nil {
		panic(err)
	}
	print(reflect.TypeOf(data).Kind().String())
	helpers.PrettyPrintObject(data)
}

func TestGetTilesData() {
	data, err := getDclTilesData()
	if err != nil {
		panic(err)
	}
	print(reflect.TypeOf(data).Kind().String())
	helpers.PrettyPrintObject(data)
}
