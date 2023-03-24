package types

import (
	"container/list"
	"context"
	"sync"

	"go.mongodb.org/mongo-driver/mongo"
)

type IDType struct {
	Version   string `bson:"version"`
	Network   string `bson:"network"`
	Event     string `bson:"event"`
	Os        string `bson:"os"`
	Timestamp string `bson:"timestamp"`
	TimeSpan  int    `bson:"timespan"`
}

type SettledType struct {
	ID       IDType      `bson:"_id"`
	Customer string      `bson:"customer"`
	Game     string      `bson:"game"`
	Value    interface{} `bson:"value"`
}

type Conn struct {
	client        *mongo.Database
	mu            sync.RWMutex
	ctx           context.Context
	gameStore     map[string]string
	store         map[string]SettledType
	customerStore map[string]string
	signal        chan int
	queue         *list.List
}
