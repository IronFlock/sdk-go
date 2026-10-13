// Reads another app's shared data (cross-app access).
//
// The provider app ("weatherstation" here) must be declared in this app's
// .ironflock/data-template.yml:
//
//	consumes:
//	  - app: weatherstation
//	    reason: "Correlates vibration with weather"
//
// and the project user must have granted access. ConnectToApp then opens a
// dedicated read-only connection to the provider's data backend.
package main

import (
	"context"
	"errors"
	"log"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

func main() {
	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		weather, err := ifl.ConnectToApp(ctx, "weatherstation", ironflock.ConnectToAppOptions{
			OnError: func(err *ironflock.CrossAppAccessError) {
				log.Printf("access to weatherstation lost: %v", err) // e.g. the grant was revoked
			},
		})
		var cerr *ironflock.CrossAppAccessError
		if errors.As(err, &cerr) {
			// NO_GRANT, PROVIDER_NOT_INSTALLED, UNKNOWN_APP or NOT_AUTHORIZED
			log.Printf("cannot access weatherstation data: %s", cerr.Code)
			return nil
		} else if err != nil {
			return err
		}

		for _, t := range weather.Tables {
			log.Printf("shared table: %s", t.Tablename)
		}
		for _, t := range weather.Transforms {
			log.Printf("shared transform: %s", t.Tablename)
		}

		rows, err := weather.GetHistory(ctx, "readings", &ironflock.TableQueryParams{Limit: 10})
		if err != nil {
			return err
		}
		log.Printf("history: %v", rows)

		// Daily averages of the last week, one row per day (a day's bucket
		// starts at the time of day the range starts).
		now := time.Now()
		avg := ironflock.SeriesMetric{Ref: "temperature", Method: ironflock.MethodAvg}
		series, err := weather.GetSeriesHistory(ctx, "readings", ironflock.SeriesQueryParams{
			Metrics:   []ironflock.SeriesMetric{avg},
			Bucket:    24 * time.Hour,
			Limit:     7,
			TimeRange: ironflock.Between(now.Add(-7*24*time.Hour), now),
		})
		if err != nil {
			return err
		}
		for _, day := range series {
			log.Printf("day starting %v: %v", day["tsp"], day[avg.Column()])
		}

		_, err = weather.SubscribeToTable(ctx, "readings", func(ev *ironflock.Event) {
			log.Printf("new reading: %v", ev.Row())
		})
		if err != nil {
			return err
		}
		<-ctx.Done()
		return nil
	})
	if err != nil {
		log.Fatal(err)
	}
}
