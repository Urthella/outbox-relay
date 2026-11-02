package main

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"strings"
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

func writeMetric(b *strings.Builder, kind, name, help string, value int64) {
	fmt.Fprintf(b, "# HELP %s %s\n# TYPE %s %s\n%s %d\n", name, help, name, kind, name, value)
}

func (m *Metrics) ServeHTTP(w http.ResponseWriter, r *http.Request) {
	var b strings.Builder
	writeMetric(&b, "counter", "outbox_relay_published_total", "Events the broker has stored, since this process started.", int64(m.published.Load()))

	s, err := m.slot(r.Context())
	up := int64(1)
	if err != nil {
		up = 0
	}
	writeMetric(&b, "gauge", "outbox_relay_database_up", "Whether the last query for the slot's state worked.", up)
	if err == nil && s.exists {
		active := int64(0)
		if s.active {
			active = 1
		}
		writeMetric(&b, "gauge", "outbox_relay_slot_active", "Whether a relay is connected to the slot.", active)
		writeMetric(&b, "gauge", "outbox_relay_slot_retained_wal_bytes", "WAL written since the slot's confirmed position.", s.retained)
	}
	w.Header().Set("Content-Type", "text/plain; version=0.0.4")
	_, _ = w.Write([]byte(b.String())) // nothing useful to do if the scraper has gone
}
