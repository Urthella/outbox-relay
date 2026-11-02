package main

import (
	"io"
	"net/http"
	"net/http/httptest"
	"strconv"
	"strings"
	"testing"
	"time"
)

func scrape(t *testing.T, m *Metrics) map[string]int64 {
	t.Helper()
	w := httptest.NewRecorder()
	m.ServeHTTP(w, httptest.NewRequest(http.MethodGet, "/metrics", nil))
	body, err := io.ReadAll(w.Result().Body)
	if err != nil {
		t.Fatal(err)
	}
	out := map[string]int64{}
	for line := range strings.Lines(string(body)) {
		if strings.HasPrefix(line, "#") {
			continue
		}
		name, value, ok := strings.Cut(strings.TrimSpace(line), " ")
		if !ok {
			t.Fatalf("bad line %q", line)
		}
		n, err := strconv.ParseInt(value, 10, 64)
		if err != nil {
			t.Fatalf("bad value in %q", line)
		}
		out[name] = n
	}
	return out
}

func TestMetricsShowWALPilingUpWhileTheRelayIsDown(t *testing.T) {
	w := newWorld(t)
	m := &Metrics{ConnString: w.url, Slot: testSlot, Publisher: w.pub}
	if got := scrape(t, m); got["outbox_relay_database_up"] != 1 {
		t.Fatalf("before the slot exists: %v", got)
	} else if _, ok := got["outbox_relay_slot_active"]; ok {
		t.Fatalf("a missing slot shouldn't report its state: %v", got)
	}

	stop := w.start(m)
	w.waitForSlot()
	w.insert("t", "k", 3)
	w.waitFor(3)
	deadline := time.Now().Add(10 * time.Second)
	for {
		got := scrape(t, m)
		if got["outbox_relay_published_total"] == 3 && got["outbox_relay_slot_active"] == 1 {
			break
		}
		if time.Now().After(deadline) {
			t.Fatalf("while running: %v", got)
		}
		time.Sleep(20 * time.Millisecond)
	}

	stop()
	before := scrape(t, m)["outbox_relay_slot_retained_wal_bytes"]
	w.insert("t", "k", 200)
	deadline = time.Now().Add(10 * time.Second)
	for {
		got := scrape(t, m)
		if got["outbox_relay_slot_active"] == 0 && got["outbox_relay_slot_retained_wal_bytes"] > before {
			return
		}
		if time.Now().After(deadline) {
			t.Fatalf("after stopping: %v, before was %d", got, before)
		}
		time.Sleep(20 * time.Millisecond)
	}
}

func TestMetricsSayWhenTheDatabaseIsUnreachable(t *testing.T) {
	m := &Metrics{ConnString: "postgres://nobody@127.0.0.1:1/none?connect_timeout=1", Slot: "s"}
	if got := scrape(t, m); got["outbox_relay_database_up"] != 0 {
		t.Fatalf("got %v", got)
	}
}
