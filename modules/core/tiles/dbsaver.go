package tiles

import (
	"context"
	"decentraland_data_downloader/modules/app/database"
	"errors"
	"go.mongodb.org/mongo-driver/bson"
	"go.mongodb.org/mongo-driver/mongo"
)

func saveMacroInDatabase(macro *MapMacro, dbInstance *mongo.Database) (*MapMacro, error) {
	dbCollection := database.CollectionInstance(dbInstance, macro)
	existing := &MapMacro{}
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"name": macro.Name, "macro_id": macro.MacroID}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return nil, err
		}
		found = false
	}
	if found {
		macro.ID = existing.ID
		err = dbCollection.UpdateWithCtx(context.Background(), macro)
		return existing, err
	} else {
		err = dbCollection.CreateWithCtx(context.Background(), macro)
		return macro, err
	}
}

func saveTileInDatabase(tile *MapTile, dbInstance *mongo.Database) error {
	dbCollection := database.CollectionInstance(dbInstance, tile)
	existing := &MapTile{}
	err := dbCollection.FirstWithCtx(context.Background(), bson.M{"coords": tile.Coords, "contract": tile.Contract, "collection": tile.Collection}, existing)
	found := true
	if err != nil {
		if !errors.Is(err, mongo.ErrNoDocuments) {
			return err
		}
		found = false
	}
	if found {
		tile.ID = existing.ID
		err = dbCollection.UpdateWithCtx(context.Background(), tile)
		return err
	} else {
		err = dbCollection.CreateWithCtx(context.Background(), tile)
		return err
	}
}
