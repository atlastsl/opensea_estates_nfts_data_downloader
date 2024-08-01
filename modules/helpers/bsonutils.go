package helpers

import "go.mongodb.org/mongo-driver/bson"

func BSONStringA(sa []string) (result bson.A) {
	result = bson.A{}
	for _, e := range sa {
		result = append(result, e)
	}
	return
}
