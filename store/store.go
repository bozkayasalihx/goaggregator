package store

import (
	"context"
	"fmt"
	"log"
	"time"

	"go.mongodb.org/mongo-driver/mongo"
	"go.mongodb.org/mongo-driver/mongo/options"
)

func NewClient(url string) (*mongo.Client, context.Context) {
	ctx := context.Background()
	client, err := mongo.Connect(ctx, options.Client().ApplyURI(url))
	if err != nil {
		log.Fatalf("couldn't connect to mongodb %v\n", err)
	}
	return client, ctx
}

func YesterdayCollection() string {
	t := time.Now()
	return fmt.Sprintf("unprocessedRawEvents%d%02d%02d", t.Year(), t.Month(), t.Day()-1)
}
