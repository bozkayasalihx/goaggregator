package tester

import (
	"context"
	"os"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func RunCollection() (context.Context, *mongo.Database) {
	var MONGO_URL string = os.Getenv("MONGO_TEST_URL")
	dbName ;= os.Getenv("DATABASE")
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(MONGO_URL))
	if err != nil {
		panic(err)
	}

	db := client.Database(dbName)
	return ctx, db
}
