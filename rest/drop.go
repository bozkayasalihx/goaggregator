package drop

import (
	"context"

	"github.com/bozkayasalih01x/go-event/types"
)

const (
	colsName = "google"
)

func DropCollection(conn *types.Conn) error {
	ctx := context.Background()
	err := conn.client.Collection(colsName).Drop(ctx)
	if err != nil {
		//TODO: drop collection  in here
		return err
	}
	return nil
}
