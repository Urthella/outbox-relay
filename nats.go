package main

import (
	"context"
	"fmt"
	"regexp"
	"strconv"
	"time"

	"github.com/nats-io/nats.go"
	"github.com/nats-io/nats.go/jetstream"
)

// invalidTopic is where events go when their topic can't be part of a NATS subject. Stopping on one
// bad row would block every event after it, and dropping it would lose it.
const invalidTopic = "_invalid"

var validTopic = regexp.MustCompile(`^[A-Za-z0-9_-]+(\.[A-Za-z0-9_-]+)*$`)

// NATSPublisher stores events in a JetStream stream. The message ID lets the stream drop a repeat
// sent after a restart, for as long as the stream's duplicate window lasts.
type NATSPublisher struct {
	js     jetstream.JetStream
	prefix string
}

// NewNATSPublisher creates or updates the stream that holds the events.
func NewNATSPublisher(ctx context.Context, nc *nats.Conn, stream, prefix string, window time.Duration) (*NATSPublisher, error) {
	js, err := jetstream.New(nc)
	if err != nil {
		return nil, err
	}
	_, err = js.CreateOrUpdateStream(ctx, jetstream.StreamConfig{
		Name:       stream,
		Subjects:   []string{prefix + ".>"},
		Duplicates: window,
	})
	if err != nil {
		return nil, fmt.Errorf("stream %s: %w", stream, err)
	}
	return &NATSPublisher{js: js, prefix: prefix}, nil
}

// Subject maps an event's topic to the subject it's published on.
func (p *NATSPublisher) Subject(topic string) string {
	if !validTopic.MatchString(topic) {
		return p.prefix + "." + invalidTopic
	}
	return p.prefix + "." + topic
}

// Publish returns once the stream has stored the event or recognized it as a repeat.
func (p *NATSPublisher) Publish(ctx context.Context, e Event) error {
	id := strconv.FormatInt(e.ID, 10)
	msg := &nats.Msg{
		Subject: p.Subject(e.Topic),
		Data:    e.Payload,
		Header: nats.Header{
			"Outbox-Id":    []string{id},
			"Outbox-Key":   []string{e.Key},
			"Outbox-Topic": []string{e.Topic},
		},
	}
	_, err := p.js.PublishMsg(ctx, msg, jetstream.WithMsgID("outbox-"+id))
	return err
}
