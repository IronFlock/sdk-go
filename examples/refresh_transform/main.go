// Appends a batch of rows and refreshes the transform the board binds to, so
// the board shows the batch at once instead of on the transform's next
// scheduled tick.
package main

import (
	"context"
	"log"
	"math/rand/v2"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

// Reading is a row of the "sensordata" table declared in the app's data
// template. "hourly_avg" is a transform of the template over it, the one a
// board widget binds to:
//
//	transforms:
//	  - tablename: hourly_avg
//	    materialize: true
//	    schedule: "0 * * * *"   # on its own, the board waits for the next full hour
//	    sql: SELECT time_bucket('1 hour', tsp) AS hour, avg(temperature) AS temperature FROM sensordata GROUP BY 1 ORDER BY 1 DESC
type Reading struct {
	Tsp         time.Time `json:"tsp"`
	Temperature float64   `json:"temperature"`
}

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		// A batch of readings. The append is committed before it returns, so
		// the refresh after it sees every row.
		batch := make([]Reading, 0, 60)
		for i := range 60 {
			batch = append(batch, Reading{
				Tsp:         time.Now().Add(time.Duration(i-60) * time.Minute),
				Temperature: 20 + rand.Float64()*5, //nolint:gosec // G404: made-up demo readings
			})
		}
		if _, err := ifl.AppendRowsToTable(ctx, "sensordata", batch); err != nil {
			return err
		}

		// Refresh once per batch, not per row: the data backend runs the
		// transform's tick now — REFRESH MATERIALIZED VIEW, then the whole
		// view to every board subscribed to it. A heavy refresh takes as long
		// as the database needs, so bound the wait with the context.
		rctx, cancel := context.WithTimeout(ctx, 30*time.Second)
		defer cancel()
		res, err := ifl.RefreshTransform(rctx, "hourly_avg")
		if err != nil {
			switch ironflock.WampURI(err) {
			case ironflock.URIRefreshInProgress:
				log.Println("a refresh the data backend started on its own holds the view; try again shortly")
			case ironflock.URIRefreshFailed, ironflock.URIStorageFull:
				log.Printf("the data backend refused the refresh: %v", err)
			}
			return err // not registered after the reconnect window: the message names the causes
		}
		switch {
		case !res.Published:
			log.Println("refreshed; no board is open, so nothing was pushed")
		case res.Skipped:
			log.Printf("the view was current already; its %d rows went out again", res.Rows)
		default:
			log.Printf("refreshed and pushed %d rows to the boards", res.Rows)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
