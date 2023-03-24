package main

import (
	"context"
	"fmt"
	"testing"
	"time"

	"go.mongodb.org/mongo-driver/bson"
)

func Test(t *testing.T) {
    ctx := context.Background()
    server := NewConnection(ctx);
    
    yesterday := DateFormat(time.Now().Add(-24 * time.Hour));
    cur, err := server.client.Collection("aggregationResults").Find(ctx, bson.D{{"_id.timestamp", yesterday.String()}}, nil)
    if err != nil {
        panic(err);
    }

    defer cur.Close(ctx);
    var results bson.D    
    for cur.Next(ctx) { 
        err := cur.Decode(&results);
        if err != nil {
            panic(err);
        }

        fmt.Printf("%v", results);
        t.Logf("%v", results);

    }


	defer server.client.Client().Disconnect(server.ctx)


}
