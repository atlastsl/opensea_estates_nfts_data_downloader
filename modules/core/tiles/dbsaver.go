package tiles

import (
	"decentraland_data_downloader/modules/app/database"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func saveMacroInDatabase(macro *MapMacro, dbInstance *mongo.Database) (*MapMacro, error) {
	dbCollection := database.CollectionInstance(dbInstance, macro)
	existing := &MapMacro{}
	err := dbCollection.First(bson.M{"name": macro.Name, "macro_id": macro.MacroID}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		found = false
	}
	if found {
		return existing, nil
	} else {
		err = dbCollection.Create(macro)
		return macro, err
	}
}

func saveTileInDatabase(tile *MapTile, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, tile)
	existing := &MapTile{}
	err := dbCollection.First(bson.M{"coords": tile.Coords, "contract": tile.Contract, "collection": tile.Collection}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		found = false
	}
	if found {
		tile.ID = existing.ID
		err = dbCollection.Update(tile)
		return err
	} else {
		err = dbCollection.Create(tile)
		return err
	}
}
