// Writes rows into the app's fleet tables: single rows, structs, bulk batches,
// an acknowledged append, and an error report.
package main

import (
	"context"
	"errors"
	"log"
	"math/rand/v2"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

// Reading is a row of the "sensordata" table declared in the app's data
// template; columns are matched by the json tags.
type Reading struct {
	Tsp         time.Time `json:"tsp"`
	Temperature float64   `json:"temperature"`
	Humidity    float64   `json:"humidity"`
}

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		// One row, as a map ... Every row carries tsp, the table's mandatory
		// timestamp column: the data backend drops a published row without it.
		row := ironflock.Row{"tsp": time.Now(), "temperature": 22.5, "humidity": 60.0}
		if err := ifl.PublishToTable(ctx, "sensordata", row); err != nil {
			return err
		}

		// ... or as a struct.
		r := Reading{Tsp: time.Now(), Temperature: 21.8, Humidity: 58}
		if err := ifl.PublishToTable(ctx, "sensordata", r); err != nil {
			return err
		}

		// Many rows in one message; the platform inserts them atomically.
		batch := make([]Reading, 0, 10)
		for i := range 10 {
			batch = append(batch, Reading{
				Tsp:         time.Now().Add(time.Duration(i) * time.Second),
				Temperature: 20 + rand.Float64()*5,
				Humidity:    50 + rand.Float64()*10,
			})
		}
		if err := ifl.PublishRowsToTable(ctx, "sensordata", batch); err != nil {
			return err
		}

		// Append waits for the insert outcome, and reports a refused row.
		res, err := ifl.AppendToTable(ctx, "sensordata", ironflock.Row{"tsp": time.Now(), "temperature": 23.1, "humidity": 61.0})
		if err != nil {
			return err
		}
		log.Printf("append result: %v", res.Value())

		// Errors land in the fleet's error-logs table.
		if _, err := ifl.ReportError(ctx, errors.New("sensor read timed out"),
			ironflock.ReportErrorOptions{Level: ironflock.LevelWarn}); err != nil {
			return err
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
