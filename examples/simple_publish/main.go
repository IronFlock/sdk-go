// Publishes an event every three seconds.
//
// Run on an IronFlock device, the SDK reads its identity from the environment
// the device agent injects. Elsewhere, set DEVICE_SERIAL_NUMBER, SWARM_KEY,
// APP_KEY, DEVICE_KEY and APP_NAME (and pass ironflock.WithURL) yourself.
package main

import (
	"context"
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
		ticker := time.NewTicker(3 * time.Second)
		defer ticker.Stop()
		for {
			if err := ifl.Publish(ctx, "test.publish.example", map[string]any{"temperature": 20}); err != nil {
				log.Print(err)
			}
			select {
			case <-ctx.Done():
				return nil
			case <-ticker.C:
			}
		}
	})
	if err != nil {
		log.Fatal(err)
	}
}
