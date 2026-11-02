package main

import (
	"context"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5"
	natsserver "github.com/nats-io/nats-server/v2/server"
	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

const (
	testSlot   = "test_slot"
	testStream = "TEST_OUTBOX"
)

type world struct {
	t   *testing.T
	url string
	db  *pgx.Conn
	js  jetstream.JetStream
	pub *NATSPublisher
}

func newWorld(t *testing.T) *world {
	t.Helper()
	base := os.Getenv("TEST_DATABASE_URL")
	if base == "" {
		t.Skip("TEST_DATABASE_URL isn't set")
	}
	ctx := context.Background()
	admin, err := pgx.Connect(ctx, base)
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() { _ = admin.Close(ctx) })
	var walLevel string
	if err := admin.QueryRow(ctx, "SHOW wal_level").Scan(&walLevel); err != nil {
		t.Fatal(err)
	}
	if walLevel != "logical" {
		t.Fatalf("the test database needs wal_level=logical, it has %s", walLevel)
	}
	name := "outbox_" + strings.ToLower(strings.NewReplacer("/", "_", "#", "_").Replace(t.Name()))
	if _, err := admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)"); err != nil {
		t.Fatal(err)
	}
	if _, err := admin.Exec(ctx, "CREATE DATABASE "+name); err != nil {
		t.Fatal(err)
	}
	u, err := url.Parse(base)
	if err != nil {
		t.Fatal(err)
	}
	u.Path = "/" + name
	db, err := pgx.Connect(ctx, u.String())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(func() {
		_ = db.Close(ctx)
		// A slot would keep WAL after the test, and the database can't go while it exists.
		deadline := time.Now().Add(10 * time.Second)
		for {
			_, err := admin.Exec(ctx, "SELECT pg_drop_replication_slot(slot_name) FROM pg_replication_slots WHERE database = $1", name)
			if err == nil || time.Now().After(deadline) {
				break
			}
			time.Sleep(100 * time.Millisecond)
		}
		_, _ = admin.Exec(ctx, "DROP DATABASE IF EXISTS "+name+" WITH (FORCE)")
	})
	schema, err := os.ReadFile("schema.sql")
	if err != nil {
		t.Fatal(err)
	}
	// The relay must cope with a topic the check constraint would have refused, so one test can
	// insert it directly: not every outbox table will have the constraint.
	ddl := strings.Replace(string(schema), ` CHECK (topic ~ '^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$')`, "", 1)
	if _, err := db.Exec(ctx, ddl); err != nil {
		t.Fatal(err)
	}

	srv, err := natsserver.NewServer(&natsserver.Options{Host: "127.0.0.1", Port: -1, JetStream: true, StoreDir: t.TempDir(), NoLog: true, NoSigs: true})
	if err != nil {
		t.Fatal(err)
	}
	srv.Start()
	t.Cleanup(srv.Shutdown)
	if !srv.ReadyForConnections(5 * time.Second) {
		t.Fatal("nats didn't start")
	}
	nc, err := nats.Connect(srv.ClientURL())
	if err != nil {
		t.Fatal(err)
	}
	t.Cleanup(nc.Close)
	js, err := jetstream.New(nc)
	if err != nil {
		t.Fatal(err)
	}
	pub, err := NewNATSPublisher(ctx, nc, testStream, "outbox", 5*time.Minute)
	if err != nil {
		t.Fatal(err)
	}
	return &world{t: t, url: u.String(), db: db, js: js, pub: pub}
}

// start runs a relay until the returned function stops it.
func (w *world) start(p Publisher) (stop func()) {
	w.t.Helper()
	ctx, cancel := context.WithCancel(context.Background())
	relay := &Relay{
		ConnString:     w.url,
		Slot:           testSlot,
		Publication:    "outbox_relay",
		Publisher:      p,
		StatusInterval: 50 * time.Millisecond,
		Log:            slog.New(slog.NewTextHandler(io.Discard, nil)),
	}
	done := make(chan struct{})
	go func() {
		defer close(done)
		_ = relay.Run(ctx)
	}()
	var stopped atomic.Bool
	stop = func() {
		if stopped.Swap(true) {
			return
		}
		cancel()
		<-done
	}
	w.t.Cleanup(stop)
	return stop
}

func (w *world) insert(topic, key string, n int) {
	w.t.Helper()
	for i := range n {
		_, err := w.db.Exec(context.Background(), "INSERT INTO outbox (topic, key, payload) VALUES ($1, $2, $3)", topic, key, fmt.Sprintf(`{"n": %d}`, i))
		if err != nil {
			w.t.Fatal(err)
		}
	}
}

type stored struct {
	id      int64
	subject string
	key     string
	payload string
}

func (w *world) messages() []stored {
	w.t.Helper()
	ctx := context.Background()
	s, err := w.js.Stream(ctx, testStream)
	if err != nil {
		w.t.Fatal(err)
	}
	info, err := s.Info(ctx)
	if err != nil {
		w.t.Fatal(err)
	}
	var out []stored
	for seq := info.State.FirstSeq; seq <= info.State.LastSeq && info.State.Msgs > 0; seq++ {
		m, err := s.GetMsg(ctx, seq)
		if err != nil {
			w.t.Fatal(err)
		}
		id, err := strconv.ParseInt(m.Header.Get("Outbox-Id"), 10, 64)
		if err != nil {
			w.t.Fatal(err)
		}
		out = append(out, stored{id, m.Subject, m.Header.Get("Outbox-Key"), string(m.Data)})
	}
	return out
}

func (w *world) waitFor(n int) []stored {
	w.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for {
		got := w.messages()
		if len(got) >= n {
			return got
		}
		if time.Now().After(deadline) {
			w.t.Fatalf("waited for %d messages, have %d", n, len(got))
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func (w *world) confirmedLSN() string {
	w.t.Helper()
	var lsn string
	err := w.db.QueryRow(context.Background(), "SELECT coalesce(confirmed_flush_lsn::text, '') FROM pg_replication_slots WHERE slot_name = $1", testSlot).Scan(&lsn)
	if errors.Is(err, pgx.ErrNoRows) {
		return ""
	}
	if err != nil {
		w.t.Fatal(err)
	}
	return lsn
}

func (w *world) waitForSlot() {
	w.t.Helper()
	deadline := time.Now().Add(15 * time.Second)
	for w.confirmedLSN() == "" {
		if time.Now().After(deadline) {
			w.t.Fatal("the slot never appeared")
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestPublishesCommittedRowsInCommitOrder(t *testing.T) {
	w := newWorld(t)
	w.start(w.pub)
	w.waitForSlot()
	w.insert("orders.created", "order-1", 5)
	got := w.waitFor(5)
	for i, m := range got {
		if m.id != int64(i+1) {
			t.Errorf("message %d has id %d", i, m.id)
		}
		if m.subject != "outbox.orders.created" || m.key != "order-1" {
			t.Errorf("message %d: subject %q, key %q", i, m.subject, m.key)
		}
		if m.payload != fmt.Sprintf(`{"n": %d}`, i) {
			t.Errorf("message %d: payload %q", i, m.payload)
		}
	}
}

func TestRolledBackRowsAreNeverPublished(t *testing.T) {
	w := newWorld(t)
	w.start(w.pub)
	w.waitForSlot()
	ctx := context.Background()
	tx, err := w.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	if _, err := tx.Exec(ctx, "INSERT INTO outbox (topic, key, payload) VALUES ('t', 'gone', '{}')"); err != nil {
		t.Fatal(err)
	}
	if err := tx.Rollback(ctx); err != nil {
		t.Fatal(err)
	}
	w.insert("t", "kept", 1)
	w.waitFor(1)
	time.Sleep(200 * time.Millisecond)
	got := w.messages()
	if len(got) != 1 || got[0].key != "kept" {
		t.Fatalf("got %+v", got)
	}
}

func TestUncommittedRowsWaitForCommit(t *testing.T) {
	w := newWorld(t)
	w.start(w.pub)
	w.waitForSlot()
	ctx := context.Background()
	tx, err := w.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() { _ = tx.Rollback(ctx) }()
	if _, err := tx.Exec(ctx, "INSERT INTO outbox (topic, key, payload) VALUES ('t', 'k', '{}')"); err != nil {
		t.Fatal(err)
	}
	time.Sleep(300 * time.Millisecond)
	if got := w.messages(); len(got) != 0 {
		t.Fatalf("published before commit: %+v", got)
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	w.waitFor(1)
}

func TestRestartCatchesUpWithoutLossOrRepeats(t *testing.T) {
	w := newWorld(t)
	stop := w.start(w.pub)
	w.waitForSlot()
	w.insert("t", "before", 3)
	w.waitFor(3)
	stop()

	w.insert("t", "while-down", 3)
	w.start(w.pub)
	w.waitFor(6)
	time.Sleep(200 * time.Millisecond)
	got := w.messages()
	if len(got) != 6 {
		t.Fatalf("got %d messages, want 6", len(got))
	}
	for i, m := range got {
		if m.id != int64(i+1) {
			t.Errorf("message %d has id %d", i, m.id)
		}
	}
}

// crashing stores events but reports failure after a given event, like a relay that dies between
// the broker's ack and confirming the slot.
type crashing struct {
	inner    Publisher
	after    int64
	crashed  atomic.Bool
	attempts atomic.Int64
}

func (c *crashing) Publish(ctx context.Context, e Event) error {
	c.attempts.Add(1)
	if err := c.inner.Publish(ctx, e); err != nil {
		return err
	}
	if e.ID == c.after && !c.crashed.Swap(true) {
		return errors.New("crash after the broker stored it")
	}
	return nil
}

func TestACrashAfterPublishingRepeatsButTheStreamDropsTheRepeats(t *testing.T) {
	w := newWorld(t)
	c := &crashing{inner: w.pub, after: 3}
	w.start(c)
	w.waitForSlot()
	ctx := context.Background()
	// One transaction, so the crash happens with the slot still before all five.
	tx, err := w.db.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	for i := range 5 {
		if _, err := tx.Exec(ctx, "INSERT INTO outbox (topic, key, payload) VALUES ('t', 'k', $1)", fmt.Sprintf(`{"n": %d}`, i)); err != nil {
			t.Fatal(err)
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	w.waitFor(5)
	time.Sleep(200 * time.Millisecond)
	got := w.messages()
	if len(got) != 5 {
		t.Fatalf("the stream holds %d messages, want 5", len(got))
	}
	if !c.crashed.Load() || c.attempts.Load() <= 5 {
		t.Fatalf("the crash didn't force a resend: crashed=%v attempts=%d", c.crashed.Load(), c.attempts.Load())
	}
}

// refusing fails while refuse is set, like a broker that's down.
type refusing struct {
	inner  Publisher
	refuse atomic.Bool
}

func (r *refusing) Publish(ctx context.Context, e Event) error {
	if r.refuse.Load() {
		return errors.New("broker down")
	}
	return r.inner.Publish(ctx, e)
}

func TestTheSlotDoesNotMovePastAnEventTheBrokerRefused(t *testing.T) {
	w := newWorld(t)
	r := &refusing{inner: w.pub}
	r.refuse.Store(true)
	w.start(r)
	w.waitForSlot()
	ctx := context.Background()
	var insertLSN string
	if err := w.db.QueryRow(ctx, "SELECT pg_current_wal_lsn()::text").Scan(&insertLSN); err != nil {
		t.Fatal(err)
	}
	w.insert("t", "k", 1)
	time.Sleep(500 * time.Millisecond)
	var moved bool
	err := w.db.QueryRow(ctx, "SELECT confirmed_flush_lsn > $1::pg_lsn FROM pg_replication_slots WHERE slot_name = $2", insertLSN, testSlot).Scan(&moved)
	if err != nil {
		t.Fatal(err)
	}
	if moved {
		t.Fatal("the slot moved past an event that was never stored")
	}
	r.refuse.Store(false)
	w.waitFor(1)
}

func TestActivityOnOtherTablesStillMovesTheSlot(t *testing.T) {
	w := newWorld(t)
	w.start(w.pub)
	w.waitForSlot()
	ctx := context.Background()
	before := w.confirmedLSN()
	if _, err := w.db.Exec(ctx, "CREATE TABLE other (v int)"); err != nil {
		t.Fatal(err)
	}
	deadline := time.Now().Add(20 * time.Second)
	for {
		if _, err := w.db.Exec(ctx, "INSERT INTO other SELECT generate_series(1, 1000)"); err != nil {
			t.Fatal(err)
		}
		if after := w.confirmedLSN(); after != before && after != "" {
			var ahead bool
			if err := w.db.QueryRow(ctx, "SELECT $1::pg_lsn > $2::pg_lsn", after, before).Scan(&ahead); err != nil {
				t.Fatal(err)
			}
			if ahead {
				return
			}
		}
		if time.Now().After(deadline) {
			t.Fatal("the slot stayed put while the database was busy, so WAL would pile up")
		}
		time.Sleep(50 * time.Millisecond)
	}
}

func TestATopicThatCantBeASubjectGoesToTheInvalidSubject(t *testing.T) {
	w := newWorld(t)
	w.start(w.pub)
	w.waitForSlot()
	w.insert("not a subject.*", "k", 1)
	w.insert("fine", "k", 1)
	got := w.waitFor(2)
	if got[0].subject != "outbox._invalid" || got[1].subject != "outbox.fine" {
		t.Fatalf("got %+v", got)
	}
}

func TestASecondRelayTakesOverWhenTheFirstStops(t *testing.T) {
	w := newWorld(t)
	stopFirst := w.start(w.pub)
	w.waitForSlot()
	w.insert("t", "first", 1)
	w.waitFor(1)

	w.start(w.pub) // the slot is in use, so this one keeps retrying
	stopFirst()
	w.insert("t", "second", 1)
	got := w.waitFor(2)
	if got[1].key != "second" {
		t.Fatalf("got %+v", got)
	}
}

func TestValidateRefusesNamesThatArentIdentifiers(t *testing.T) {
	if err := (&Relay{Slot: "bad-name", Publication: "x"}).validate(); err == nil {
		t.Error("a slot name that isn't an identifier should be refused")
	}
	if err := (&Relay{Slot: "x", Publication: "x'; DROP"}).validate(); err == nil {
		t.Error("a publication name that isn't an identifier should be refused")
	}
}

func TestDecodeRefusesATableWithoutTheColumns(t *testing.T) {
	rel := &pglogrepl.RelationMessage{RelationName: "wrong", Columns: []*pglogrepl.RelationMessageColumn{{Name: "id"}}}
	tuple := &pglogrepl.TupleData{Columns: []*pglogrepl.TupleDataColumn{{Data: []byte("1")}}}
	if _, err := decode(rel, tuple); err == nil {
		t.Fatal("a table without topic, key and payload should be refused")
	}
}
