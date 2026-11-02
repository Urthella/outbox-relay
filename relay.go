package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"regexp"
	"strconv"
	"time"

	"github.com/jackc/pglogrepl"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgproto3"
)

// Event is one committed row of the outbox table.
type Event struct {
	ID      int64
	Topic   string
	Key     string
	Payload []byte
}

// Publisher delivers an event. It must return only once the broker has stored it, because a nil
// error is what lets the relay move the replication slot past that event.
type Publisher interface {
	Publish(ctx context.Context, e Event) error
}

// Relay streams inserts on the outbox table from a logical replication slot to a Publisher.
type Relay struct {
	ConnString     string
	Slot           string
	Publication    string
	Publisher      Publisher
	StatusInterval time.Duration
	Log            *slog.Logger
}

const duplicateObject = "42710"

var identifier = regexp.MustCompile(`^[a-z_][a-z0-9_]*$`)

func (r *Relay) validate() error {
	if !identifier.MatchString(r.Slot) {
		return fmt.Errorf("invalid slot name %q", r.Slot)
	}
	if !identifier.MatchString(r.Publication) {
		return fmt.Errorf("invalid publication name %q", r.Publication)
	}
	return nil
}

// Run keeps the relay going until ctx ends, reconnecting after a failure. A restart resumes from
// the slot's confirmed position, so anything not yet confirmed is sent again.
func (r *Relay) Run(ctx context.Context) error {
	if err := r.validate(); err != nil {
		return err
	}
	backoff := time.Second
	for {
		started := time.Now()
		err := r.runOnce(ctx)
		if ctx.Err() != nil {
			return ctx.Err()
		}
		if time.Since(started) > time.Minute {
			backoff = time.Second
		}
		r.logger().Error("relay stopped, will retry", "err", err, "in", backoff)
		select {
		case <-ctx.Done():
			return ctx.Err()
		case <-time.After(backoff):
		}
		backoff = min(backoff*2, 30*time.Second)
	}
}

func (r *Relay) logger() *slog.Logger {
	if r.Log != nil {
		return r.Log
	}
	return slog.Default()
}

func (r *Relay) runOnce(ctx context.Context) error {
	cfg, err := pgconn.ParseConfig(r.ConnString)
	if err != nil {
		return fmt.Errorf("parse connection string: %w", err)
	}
	cfg.RuntimeParams["replication"] = "database"
	conn, err := pgconn.ConnectConfig(ctx, cfg)
	if err != nil {
		return fmt.Errorf("connect: %w", err)
	}
	defer func() {
		closeCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		_ = conn.Close(closeCtx)
	}()

	_, err = pglogrepl.CreateReplicationSlot(ctx, conn, r.Slot, "pgoutput", pglogrepl.CreateReplicationSlotOptions{})
	var pgErr *pgconn.PgError
	if err != nil && (!errors.As(err, &pgErr) || pgErr.Code != duplicateObject) {
		return fmt.Errorf("create slot: %w", err)
	}

	// Position 0 means "where the slot's confirmed position is", which is what makes a restart safe.
	err = pglogrepl.StartReplication(ctx, conn, r.Slot, 0, pglogrepl.StartReplicationOptions{
		PluginArgs: []string{"proto_version '1'", "publication_names '" + r.Publication + "'"},
	})
	if err != nil {
		return fmt.Errorf("start replication: %w", err)
	}
	r.logger().Info("streaming", "slot", r.Slot, "publication", r.Publication)
	return r.stream(ctx, conn)
}

func (r *Relay) stream(ctx context.Context, conn *pgconn.PgConn) error {
	interval := r.StatusInterval
	if interval == 0 {
		interval = 5 * time.Second
	}
	var (
		confirmed pglogrepl.LSN
		inTx      bool
		relations = map[uint32]*pglogrepl.RelationMessage{}
		nextSend  = time.Now().Add(interval)
	)
	report := func() error {
		nextSend = time.Now().Add(interval)
		return pglogrepl.SendStandbyStatusUpdate(ctx, conn, pglogrepl.StandbyStatusUpdate{WALWritePosition: confirmed})
	}

	for {
		if !time.Now().Before(nextSend) {
			if err := report(); err != nil {
				return fmt.Errorf("send status: %w", err)
			}
		}
		recvCtx, cancel := context.WithDeadline(ctx, nextSend)
		raw, err := conn.ReceiveMessage(recvCtx)
		cancel()
		if err != nil {
			if pgconn.Timeout(err) && ctx.Err() == nil {
				continue
			}
			return fmt.Errorf("receive: %w", err)
		}
		switch msg := raw.(type) {
		case *pgproto3.ErrorResponse:
			return fmt.Errorf("server: %w", pgconn.ErrorResponseToPgError(msg))
		case *pgproto3.CopyData:
			if len(msg.Data) == 0 {
				continue
			}
			switch msg.Data[0] {
			case pglogrepl.PrimaryKeepaliveMessageByteID:
				ka, err := pglogrepl.ParsePrimaryKeepaliveMessage(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse keepalive: %w", err)
				}
				// Outside a transaction, everything before this position has reached us, so a busy
				// database with a quiet outbox doesn't keep WAL forever.
				if !inTx && ka.ServerWALEnd > confirmed {
					confirmed = ka.ServerWALEnd
				}
				if ka.ReplyRequested {
					if err := report(); err != nil {
						return fmt.Errorf("send status: %w", err)
					}
				}
			case pglogrepl.XLogDataByteID:
				x, err := pglogrepl.ParseXLogData(msg.Data[1:])
				if err != nil {
					return fmt.Errorf("parse xlog data: %w", err)
				}
				logical, err := pglogrepl.Parse(x.WALData)
				if err != nil {
					return fmt.Errorf("parse logical message: %w", err)
				}
				switch m := logical.(type) {
				case *pglogrepl.RelationMessage:
					relations[m.RelationID] = m
				case *pglogrepl.BeginMessage:
					inTx = true
				case *pglogrepl.InsertMessage:
					rel, ok := relations[m.RelationID]
					if !ok {
						return fmt.Errorf("insert for unknown relation %d", m.RelationID)
					}
					e, err := decode(rel, m.Tuple)
					if err != nil {
						return err
					}
					if err := r.Publisher.Publish(ctx, e); err != nil {
						return fmt.Errorf("publish event %d: %w", e.ID, err)
					}
				case *pglogrepl.CommitMessage:
					// Only here, with every event of the transaction stored, is it safe to move on.
					inTx = false
					confirmed = m.TransactionEndLSN
					if err := report(); err != nil {
						return fmt.Errorf("send status: %w", err)
					}
				}
			}
		}
	}
}

func decode(rel *pglogrepl.RelationMessage, tuple *pglogrepl.TupleData) (Event, error) {
	if tuple == nil || len(tuple.Columns) != len(rel.Columns) {
		return Event{}, fmt.Errorf("table %s: tuple doesn't match its columns", rel.RelationName)
	}
	var (
		e       Event
		found   int
		missing = errors.New("needs columns id, topic, key and payload")
	)
	for i, col := range rel.Columns {
		value := tuple.Columns[i].Data
		switch col.Name {
		case "id":
			id, err := strconv.ParseInt(string(value), 10, 64)
			if err != nil {
				return Event{}, fmt.Errorf("table %s: id: %w", rel.RelationName, err)
			}
			e.ID = id
		case "topic":
			e.Topic = string(value)
		case "key":
			e.Key = string(value)
		case "payload":
			e.Payload = value
		default:
			continue
		}
		found++
	}
	if found != 4 {
		return Event{}, fmt.Errorf("table %s: %w", rel.RelationName, missing)
	}
	return e, nil
}
