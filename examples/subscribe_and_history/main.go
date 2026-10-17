// Streams the stored rows of a table and reads its history: the latest value
// per entity, a filtered range, and a down-sampled series.
package main

import (
	"context"
	"log"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

// Reading is a row of the "sensordata" table; columns are matched by the json
// tags. The data backend returns timestamps as epoch milliseconds.
type Reading struct {
	Tsp         int64   `json:"tsp"`
	DeviceKey   int     `json:"device_key"`
	Temperature float64 `json:"temperature"`
}

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		// Realtime: one event per stored row, bulk inserts included. The
		// subscription survives reconnects; the rows stored while the
		// connection was down arrive after it.
		ts, err := ifl.SubscribeToTable(ctx, "sensordata", func(ev *ironflock.Event) {
			log.Printf("new row: %v", ev.Row())
		})
		if err != nil {
			return err
		}
		log.Printf("following table %s (live: %v)", ts.Table(), ts.Active())

		// Current value per entity.
		latest, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
			Limit:     100,
			FilterAnd: []ironflock.Filter{ironflock.Latest()},
		})
		if err != nil {
			return err
		}
		readings, err := ironflock.DecodeRows[Reading](latest)
		if err != nil {
			return err
		}
		for _, r := range readings {
			log.Printf("device %d: %.1f°C at %s", r.DeviceKey, r.Temperature, time.UnixMilli(r.Tsp).UTC())
		}

		// The last day, warm readings only.
		warm, err := ifl.GetHistory(ctx, "sensordata", &ironflock.TableQueryParams{
			Limit:     500,
			TimeRange: ironflock.Since(time.Now().Add(-24 * time.Hour)),
			FilterAnd: []ironflock.Filter{ironflock.Where("temperature", ">", 25)},
			Columns:   []string{"temperature"},
		})
		if err != nil {
			return err
		}
		log.Printf("%d warm readings", len(warm))

		// Hourly averages for a chart: one row per hour, its start in tsp
		// (epoch ms), the average under "AVG:temperature". The range holds
		// exactly Limit buckets; one any longer (an open end, which the data
		// backend reads as its own now) would make it widen the buckets.
		now := time.Now()
		avg := ironflock.SeriesMetric{Ref: "temperature", Method: ironflock.MethodAvg}
		series, err := ifl.GetSeriesHistory(ctx, "sensordata", ironflock.SeriesQueryParams{
			Metrics:   []ironflock.SeriesMetric{avg},
			Bucket:    time.Hour,
			Limit:     24,
			TimeRange: ironflock.Between(now.Add(-24*time.Hour), now),
		})
		if err != nil {
			return err
		}
		for _, bucket := range series {
			log.Printf("hour starting %v: %v", bucket["tsp"], bucket[avg.Column()])
		}

		<-ctx.Done() // keep streaming until SIGINT/SIGTERM

		// Run removes every subscription once main returns; a stream that
		// is to end earlier ends like this: at once, both feeds, without
		// waiting for a connection (ts.Unsubscribe(ctx) does the same).
		cleanup, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := ifl.Unsubscribe(cleanup, ts); err != nil {
			log.Printf("unsubscribing from %s: %v", ts.Table(), err)
		}
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
