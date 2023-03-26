package main

import (
	"flag"
	"fmt"
	"log"
	"math/rand"
	"os"
	"time"

	"github.com/bozkayasalih01x/go-event/store"
	"github.com/joho/godotenv"
	"go.mongodb.org/mongo-driver/bson"
)

var URL string
var DB string

var Collection string
var Year int
var Month int
var Day int

func main() {
	flag.StringVar(&Collection, "col", "aggregationResults", "the collection name of you want to working on")
	flag.IntVar(&Year, "year", 2023, "the year you want to working on")
	flag.IntVar(&Month, "month", 3, "the month you want to working on")
	flag.IntVar(&Day, "day", 24, "the day you want to working on")
	flag.Parse()
	rand.Seed(time.Now().UnixNano())
	err := godotenv.Load("../app.env")

	if err != nil {
		panic(err)
	}
	URL = os.Getenv("MONGO_URL")
	DB = os.Getenv("DATABASE")
	client, ctx := store.NewClient(URL)
	db := client.Database(DB)

	date := bson.D{{"_id.timestamp", time.Date(Year, time.Month(Month), Day, 0, 0, 0, 0, time.UTC)}}
	fmt.Printf("running on %v", date)
	result, err := db.Collection(Collection).DeleteMany(ctx, date)
	if err != nil {
		log.Fatalf("couldnt delete collections: %v", err)
	}
	fmt.Printf("total delete of collections: %d", result.DeletedCount)
}
