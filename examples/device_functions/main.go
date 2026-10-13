// Registers a function other devices of the app (and dashboard widget
// actions) can call, and calls the same function on another device.
//
//	go run . -peer 42   # call "add" on device 42 every 5 seconds
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"time"

	ironflock "github.com/ironflock/sdk-go"
)

func main() {
	peer := flag.Int("peer", 0, "device key of a peer device to call")
	flag.Parse()

	ifl, err := ironflock.New()
	if err != nil {
		log.Fatal(err)
	}
	err = ifl.Run(context.Background(), func(ctx context.Context) error {
		// Registered as <SWARM_KEY>.<DEVICE_KEY>.<APP_KEY>.<STAGE>.add and
		// restored after every reconnect.
		_, err := ifl.RegisterDeviceFunction(ctx, "add", func(ctx context.Context, inv *ironflock.Invocation) (any, error) {
			if len(inv.Args) != 2 {
				return nil, &ironflock.WampError{URI: "com.example.error.bad_arguments", Args: []any{"add takes two numbers"}}
			}
			a, ok1 := inv.Args[0].(int64)
			b, ok2 := inv.Args[1].(int64)
			if !ok1 || !ok2 {
				return nil, fmt.Errorf("add takes two integers, got %T and %T", inv.Args[0], inv.Args[1])
			}
			return a + b, nil
		})
		if err != nil {
			return err
		}
		if *peer == 0 {
			<-ctx.Done()
			return nil
		}

		ticker := time.NewTicker(5 * time.Second)
		defer ticker.Stop()
		for {
			res, err := ifl.CallDeviceFunction(ctx, *peer, "add", 1, 2)
			if err != nil {
				log.Printf("call failed (%s): %v", ironflock.WampURI(err), err)
			} else {
				log.Printf("1 + 2 = %v", res.Value())
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
