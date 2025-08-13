package metaverses

import (
	"go.mongodb.org/mongo-driver/mongo"
	"strings"
)

func processMetaverseExtraData(task string, params map[string]any, metaverse MetaverseName, dbInstance *mongo.Database) error {
	var err error
	if metaverse == MetaverseDcl {
		err = processDclTaskExtra(task, params, dbInstance)
	} else if metaverse == MetaverseSnd {
		err = processTaskExtraSolo(task, params, dbInstance)
	} else if metaverse == MetaverseSmn {
		err = processSmnTaskExtra(task, params, dbInstance)
	}
	return err
}

func processMetaverseAssetData(task string, params map[string]any, metaverse MetaverseName, dbInstance *mongo.Database) error {
	var assets []*MetaverseAsset
	var err error
	if metaverse == MetaverseDcl {
		assets, err = processDclTaskAsset(task, params, dbInstance)
	} else if metaverse == MetaverseSnd {
		assets, err = processTaskAssetSolo(task, params, dbInstance)
	} else if metaverse == MetaverseSmn {
		assets, err = processSmnTaskAsset(task, params, dbInstance)
	}
	if err != nil {
		return err
	}
	if assets != nil {
		err = saveMetaverseAssetsInDatabase(assets, dbInstance)
		if err != nil {
			return err
		}
	}
	return nil
}

func processMetaverseData(task string, params map[string]any, metaverse MetaverseName, dbInstance *mongo.Database) error {
	target := strings.Split(task, ":")[0]
	if target == "extra" {
		return processMetaverseExtraData(task, params, metaverse, dbInstance)
	} else {
		return processMetaverseAssetData(task, params, metaverse, dbInstance)
	}
}

func getterRequestsOrder(metaverse MetaverseName) ([]string, error) {
	if metaverse == MetaverseDcl {
		return getterDclRequestOrder()
	} else if metaverse == MetaverseSnd {
		return getterSndRequestsOrder()
	} else if metaverse == MetaverseSmn {
		return getterSmnRequestsOrder()
	}
	return nil, nil
}

func getterAdditionalData(metaverse MetaverseName, dbInstance *mongo.Database) (map[string]any, error) {
	mtvInfo, err := getMetaverseInfo(metaverse, dbInstance)
	if err != nil {
		return nil, err
	}
	if metaverse == MetaverseDcl {
		return map[string]any{"metaverseInfo": mtvInfo}, nil
	} else if metaverse == MetaverseSnd {
		neighborhoodsList, e1 := getSndNeighborhoodList()
		if e1 != nil {
			return nil, e1
		}
		themesList, e2 := getSndThemeList()
		if e2 != nil {
			return nil, e2
		}
		genresList, e3 := getSndGenreList()
		if e3 != nil {
			return nil, e3
		}
		return map[string]any{"metaverseInfo": mtvInfo, "neighborhoods": neighborhoodsList, "themes": themesList, "genres": genresList}, nil
	} else if metaverse == MetaverseSmn {
		rawLandItemsList, e1 := getSmnLandItemList()
		if e1 != nil {
			return nil, e1
		}
		return map[string]any{"metaverseInfo": mtvInfo, "landItemsList": rawLandItemsList}, nil
	}
	return map[string]any{}, nil
}
