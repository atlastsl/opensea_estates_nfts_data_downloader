package main

import (
	"github.com/joho/godotenv"
)

func main0() {
	err := godotenv.Load(".env")
	if err != nil {
		panic(err)
	}
	//collections.SaveInfo(collections.DecentralandCollectionInfo)
}
