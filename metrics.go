package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"sync/atomic"
	"time"

	"github.com/jackc/pgx/v5"
)

// Metrics serves Prometheus metrics. The most important is retained WAL: a stopped relay makes
// Postgres keep WAL for its slot, and left alone that fills the disk.
type Metrics struct {
	ConnString string
	Slot       string
	Publisher  Publisher
	published  atomic.Uint64
}

// Publish counts the events the broker has stored.
func (m *Metrics) Publish(ctx context.Context, e Event) error {
	if err := m.Publisher.Publish(ctx, e); err != nil {
		return err
	}
	m.published.Add(1)
	return nil
}

type slotState struct {
	exists   bool
	active   bool
	retained int64
}

// ponytail: a new connection per scrape. That's fine at a scrape every few seconds, and it keeps the
// relay from holding a second connection open just for this.
func (m *Metrics) slot(ctx context.Context) (slotState, error) {
	ctx, cancel := context.WithTimeout(ctx, 5*time.Second)
	defer cancel()
	conn, err := pgx.Connect(ctx, m.ConnString)
	if err != nil {
		return slotState{}, err
	}
	defer func() { _ = conn.Close(context.Background()) }()
	s := slotState{exists: true}
	err = conn.QueryRow(ctx, `
		SELECT active, coalesce(pg_wal_lsn_diff(pg_current_wal_lsn(), confirmed_flush_lsn), 0)::bigint
		FROM pg_replication_slots WHERE slot_name = $1`, m.Slot).Scan(&s.active, &s.retained)
	if errors.Is(err, pgx.ErrNoRows) {
		return slotState{}, nil
	}
	return s, err
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	fmt.Fprintln(w, "# HELP outbox_relay_published_total Events the broker has stored, since this process started.")
	fmt.Fprintln(w, "# TYPE outbox_relay_published_total counter")
	fmt.Fprintf(w, "outbox_relay_published_total %d\n", m.published.Load())

	s, err := m.slot(r.Context())
	up := 1
	if err != nil {
		up = 0
	}
	fmt.Fprintln(w, "# HELP outbox_relay_database_up Whether the last query for the slot's state worked.")
	fmt.Fprintln(w, "# TYPE outbox_relay_database_up gauge")
	fmt.Fprintf(w, "outbox_relay_database_up %d\n", up)
	if err != nil || !s.exists {
		return
	}
	active := 0
	if s.active {
		active = 1
	}
	fmt.Fprintln(w, "# HELP outbox_relay_slot_active Whether a relay is connected to the slot.")
	fmt.Fprintln(w, "# TYPE outbox_relay_slot_active gauge")
	fmt.Fprintf(w, "outbox_relay_slot_active %d\n", active)
	fmt.Fprintln(w, "# HELP outbox_relay_slot_retained_wal_bytes WAL written since the slot's confirmed position.")
	fmt.Fprintln(w, "# TYPE outbox_relay_slot_retained_wal_bytes gauge")
	fmt.Fprintf(w, "outbox_relay_slot_retained_wal_bytes %d\n", s.retained)
}
