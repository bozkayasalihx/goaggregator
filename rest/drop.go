package drop

import (
	"fmt"
	"time"

	"github.com/bozkayasalih01x/go-event/types"
)

const (
	colsName = "google"
)

func DropCollection(conn *types.Conn) error {
 //    ctx := context.Background();
	// err := conn.client.Collection(colsName).Drop(ctx)
	// if err != nil {
	// 	//TODO: drop collection  in here
	// 	return err
	// }
	return nil
}

func YesterdayCollection() string {
    t := time.Now()
    return fmt.Sprintf("unprocessedRawEvents%d%02d%02d", t.Year(), t.Month(), t.Day() -1);
}
