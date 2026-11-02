// Command outbox-relay streams committed rows of a Postgres outbox table to NATS JetStream.
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/nats-io/nats.go"
)

func env(name, fallback string) string {
	if v := os.Getenv(name); v != "" {
		return v
	}
	return fallback
}

func main() {
	var (
		database    = flag.String("database", os.Getenv("DATABASE_URL"), "Postgres connection string (or DATABASE_URL)")
		natsURL     = flag.String("nats", env("NATS_URL", nats.DefaultURL), "NATS server (or NATS_URL)")
		slot        = flag.String("slot", "outbox_relay", "replication slot, created if missing")
		publication = flag.String("publication", "outbox_relay", "publication covering the outbox table")
		stream      = flag.String("stream", "OUTBOX", "JetStream stream, created if missing")
		prefix      = flag.String("prefix", "outbox", "subject prefix: events go to <prefix>.<topic>")
		window      = flag.Duration("dedup-window", 10*time.Minute, "how long the stream remembers message IDs")
	)
	flag.Parse()
	if err := run(*database, *natsURL, *slot, *publication, *stream, *prefix, *window); err != nil && !errors.Is(err, context.Canceled) {
		fmt.Fprintln(os.Stderr, "outbox-relay:", err)
		os.Exit(1)
	}
}

func run(database, natsURL, slot, publication, stream, prefix string, window time.Duration) error {
	if database == "" {
		return errors.New("set -database or DATABASE_URL")
	}
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	nc, err := nats.Connect(natsURL, nats.MaxReconnects(-1))
	if err != nil {
		return fmt.Errorf("connect to nats: %w", err)
	}
	defer nc.Close()
	publisher, err := NewNATSPublisher(ctx, nc, stream, prefix, window)
	if err != nil {
		return err
	}
	relay := &Relay{ConnString: database, Slot: slot, Publication: publication, Publisher: publisher, Log: slog.Default()}
	return relay.Run(ctx)
}
