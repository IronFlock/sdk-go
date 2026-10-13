// Uses the underlying self-healing WAMP connection directly, without the
// IronFlock facade. The realm comes from SWARM_KEY, APP_KEY and ENV, as the
// device agent injects them into every app container.
package main

import (
	"context"
	"fmt"
	"log"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"github.com/ironflock/sdk-go/wamp"
)

func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}

func run() error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	swarmKey, err := strconv.Atoi(os.Getenv("SWARM_KEY"))
	if err != nil {
		return fmt.Errorf("SWARM_KEY: %w", err)
	}
	appKey, err := strconv.Atoi(os.Getenv("APP_KEY"))
	if err != nil {
		return fmt.Errorf("APP_KEY: %w", err)
	}

	conn := wamp.NewConnection()
	err = conn.Configure(wamp.Config{
		SwarmKey: swarmKey,
		AppKey:   appKey,
		Stage:    wamp.StageFromEnv(os.Getenv("ENV")),
		// URL, serial number and credential come from the environment when
		// left empty.
		OnConnect:    func() { log.Print("connected") },
		OnDisconnect: func(reason string) { log.Printf("disconnected: %s", reason) },
	})
	if err != nil {
		return err
	}
	if err := conn.Start(ctx); err != nil {
		return err
	}
	defer func() {
		stopCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := conn.Stop(stopCtx); err != nil {
			log.Printf("stop: %v", err)
		}
	}()

	// The router does not send a publisher its own events (see
	// PublishOptions.ExcludeMe): this handler sees other sessions' events.
	_, err = conn.Subscribe(ctx, "com.example.status", func(ev *wamp.Event) {
		log.Printf("status: %v %v", ev.Args, ev.Kwargs)
	}, nil)
	if err != nil {
		return err
	}
	err = conn.Publish(ctx, "com.example.status", []any{"online"}, nil,
		&wamp.PublishOptions{Acknowledge: true}, 0)
	if err != nil {
		return err
	}
	<-ctx.Done()
	return nil
}
