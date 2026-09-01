//go:build linux

// Package client provides a reusable, serialized Unix RPC client for the daemon.
package client

import (
	"context"
	"crypto/rand"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"net"
	"sync"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/protocol"
)

const (
	defaultDialTimeout  = 3 * time.Second
	defaultReadTimeout  = 30 * time.Second
	defaultWriteTimeout = 30 * time.Second
)

var ErrClosed = errors.New("daemon client is closed")

// RPCError is a daemon-declared error. It intentionally includes no response body.
type RPCError struct{ Code, Message string }

func (e *RPCError) Error() string { return e.Code + ": " + e.Message }

// Options configures progress deadlines. Non-positive values use safe defaults.
type Options struct{ DialTimeout, ReadTimeout, WriteTimeout time.Duration }

// Health is the daemon health response.
type Health struct {
	Version int    `json:"version"`
	Status  string `json:"status"`
}

// Info is the daemon information response.
type Info struct {
	Version int `json:"version"`
}

// EventReference identifies an event without exposing its evidence.
type EventReference struct {
	ID         string `json:"id"`
	ProjectID  string `json:"project_id"`
	SessionID  string `json:"session_id"`
	ExchangeID string `json:"exchange_id"`
	Sequence   int64  `json:"sequence"`
}

// PayloadMetadata describes an exact paginated read.
type PayloadMetadata struct {
	EventID      string `json:"event_id"`
	TotalSize    uint64 `json:"total_size"`
	SHA256       string `json:"sha256"`
	Offset       uint64 `json:"offset"`
	Limit        uint64 `json:"limit"`
	BytesWritten uint64 `json:"bytes_written"`
}

// EventMetadata identifies an event and its payload integrity data without reading evidence bytes.
type EventMetadata struct {
	EventID   string `json:"event_id"`
	TotalSize uint64 `json:"total_size"`
	SHA256    string `json:"sha256"`
}

// Client reuses one connection. Every operation owns the connection until its complete response is consumed.
type Client struct {
	socket  string
	options Options
	gate    chan struct{}
	mu      sync.Mutex
	conn    net.Conn
	closed  bool
}

func New(socket string, options Options) *Client {
	if options.DialTimeout <= 0 {
		options.DialTimeout = defaultDialTimeout
	}
	if options.ReadTimeout <= 0 {
		options.ReadTimeout = defaultReadTimeout
	}
	if options.WriteTimeout <= 0 {
		options.WriteTimeout = defaultWriteTimeout
	}
	return &Client{socket: socket, options: options, gate: make(chan struct{}, 1)}
}

func contextOrBackground(ctx context.Context) context.Context {
	if ctx == nil {
		return context.Background()
	}
	return ctx
}

func (c *Client) acquire(ctx context.Context) error {
	ctx = contextOrBackground(ctx)
	select {
	case c.gate <- struct{}{}:
		return nil
	case <-ctx.Done():
		return ctx.Err()
	}
}
func (c *Client) release() { <-c.gate }

// Close is idempotent and interrupts any operation currently blocked on the socket.
func (c *Client) Close() error {
	c.mu.Lock()
	defer c.mu.Unlock()
	c.closed = true
	if c.conn == nil {
		return nil
	}
	err := c.conn.Close()
	c.conn = nil
	return err
}
func (c *Client) poison() {
	c.mu.Lock()
	if c.conn != nil {
		_ = c.conn.Close()
		c.conn = nil
	}
	c.mu.Unlock()
}

func (c *Client) connection(ctx context.Context) (net.Conn, error) {
	c.mu.Lock()
	if c.closed {
		c.mu.Unlock()
		return nil, ErrClosed
	}
	if c.conn != nil {
		conn := c.conn
		c.mu.Unlock()
		return conn, nil
	}
	c.mu.Unlock()
	dialCtx, cancel := withTimeout(ctx, c.options.DialTimeout)
	defer cancel()
	conn, err := (&net.Dialer{}).DialContext(dialCtx, "unix", c.socket)
	if err != nil {
		return nil, fmt.Errorf("dial daemon: %w", err)
	}
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.closed {
		_ = conn.Close()
		return nil, ErrClosed
	}
	if c.conn == nil {
		c.conn = conn
		return conn, nil
	}
	_ = conn.Close()
	return c.conn, nil
}
func withTimeout(ctx context.Context, timeout time.Duration) (context.Context, context.CancelFunc) {
	return context.WithTimeout(contextOrBackground(ctx), timeout)
}
func (c *Client) deadlines(ctx context.Context, conn net.Conn) func() {
	read := time.Now().Add(c.options.ReadTimeout)
	write := time.Now().Add(c.options.WriteTimeout)
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(read) {
		read = deadline
	}
	if deadline, ok := ctx.Deadline(); ok && deadline.Before(write) {
		write = deadline
	}
	_ = conn.SetReadDeadline(read)
	_ = conn.SetWriteDeadline(write)
	done := make(chan struct{})
	if ctx.Done() != nil {
		go func() {
			select {
			case <-ctx.Done():
				_ = conn.SetDeadline(time.Now())
			case <-done:
			}
		}()
	}
	return func() { close(done) }
}
func requestID() (string, error) {
	data := make([]byte, 16)
	if _, err := io.ReadFull(rand.Reader, data); err != nil {
		return "", err
	}
	return hex.EncodeToString(data), nil
}

func (c *Client) call(ctx context.Context, operation string, body any, response any) error {
	ctx = contextOrBackground(ctx)
	if err := c.acquire(ctx); err != nil {
		return err
	}
	defer c.release()
	if err := ctx.Err(); err != nil {
		return err
	}
	conn, err := c.connection(ctx)
	if err != nil {
		return err
	}
	stopCancel := c.deadlines(ctx, conn)
	defer stopCancel()
	id, err := requestID()
	if err != nil {
		return fmt.Errorf("generate request ID: %w", err)
	}
	raw, err := json.Marshal(body)
	if err != nil {
		return fmt.Errorf("encode request: %w", err)
	}
	if err = protocol.Encode(conn, protocol.Request{ProtocolVersion: protocol.Version, RequestID: id, Operation: operation, SentAt: time.Now().UTC(), Body: raw}); err != nil {
		c.poison()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("write daemon request: %w", err)
	}
	var envelope protocol.Response
	if err = protocol.Decode(conn, &envelope); err != nil {
		c.poison()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return ctxErr
		}
		return fmt.Errorf("read daemon response: %w", err)
	}
	if err = validateResponse(envelope, id); err != nil {
		c.poison()
		return err
	}
	if envelope.Status == "error" {
		return &RPCError{Code: envelope.Error.Code, Message: envelope.Error.Message}
	}
	if err = json.Unmarshal(envelope.Body, response); err != nil {
		c.poison()
		return fmt.Errorf("decode daemon response: %w", err)
	}
	return nil
}
func validateResponse(response protocol.Response, id string) error {
	if response.ProtocolVersion != protocol.Version {
		return errors.New("daemon response protocol version is incompatible")
	}
	if response.RequestID != id {
		return errors.New("daemon response request ID does not match")
	}
	switch response.Status {
	case "ok":
		if response.Error != nil {
			return errors.New("successful daemon response contains an error")
		}
	case "error":
		if response.Error == nil || response.Error.Code == "" || response.Error.Message == "" {
			return errors.New("daemon error response is invalid")
		}
	default:
		return errors.New("daemon response status is invalid")
	}
	return nil
}
func (c *Client) Health(ctx context.Context) (Health, error) {
	var out Health
	err := c.call(ctx, "system.health", map[string]any{}, &out)
	return out, err
}
func (c *Client) Info(ctx context.Context) (Info, error) {
	var out Info
	err := c.call(ctx, "system.info", map[string]any{}, &out)
	return out, err
}
func (c *Client) Search(ctx context.Context, query string, limit int) ([]EventReference, error) {
	if query == "" || limit < 0 {
		return nil, errors.New("invalid search arguments")
	}
	var out struct {
		Events []EventReference `json:"events"`
	}
	err := c.call(ctx, "search.events", map[string]any{"query": query, "limit": limit}, &out)
	return out.Events, err
}

// EventMetadata retrieves event integrity metadata without reading evidence bytes.
func (c *Client) EventMetadata(ctx context.Context, eventID string) (EventMetadata, error) {
	if eventID == "" {
		return EventMetadata{}, errors.New("invalid event metadata arguments")
	}
	var out EventMetadata
	err := c.call(ctx, "event.metadata", map[string]any{"event_id": eventID}, &out)
	if err != nil {
		return EventMetadata{}, err
	}
	if out.EventID != eventID || len(out.SHA256) != 64 {
		return EventMetadata{}, errors.New("event metadata is invalid")
	}
	if _, err := hex.DecodeString(out.SHA256); err != nil {
		return EventMetadata{}, errors.New("event metadata hash is invalid")
	}
	return out, nil
}

// ReadPayload streams one evidence data-plane frame directly to destination. A protocol/data-plane error poisons the connection.
func (c *Client) ReadPayload(ctx context.Context, eventID string, offset, limit uint64, destination io.Writer) (PayloadMetadata, error) {
	ctx = contextOrBackground(ctx)
	if eventID == "" || destination == nil {
		return PayloadMetadata{}, errors.New("invalid payload arguments")
	}
	if err := c.acquire(ctx); err != nil {
		return PayloadMetadata{}, err
	}
	defer c.release()
	if err := ctx.Err(); err != nil {
		return PayloadMetadata{}, err
	}
	conn, err := c.connection(ctx)
	if err != nil {
		return PayloadMetadata{}, err
	}
	stopCancel := c.deadlines(ctx, conn)
	defer stopCancel()
	id, err := requestID()
	if err != nil {
		return PayloadMetadata{}, err
	}
	raw, _ := json.Marshal(map[string]any{"event_id": eventID, "offset": offset, "limit": limit})
	if err = protocol.Encode(conn, protocol.Request{ProtocolVersion: protocol.Version, RequestID: id, Operation: "event.read_payload", SentAt: time.Now().UTC(), Body: raw}); err != nil {
		c.poison()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return PayloadMetadata{}, ctxErr
		}
		return PayloadMetadata{}, fmt.Errorf("write payload request: %w", err)
	}
	var envelope protocol.Response
	if err = protocol.Decode(conn, &envelope); err != nil {
		c.poison()
		if ctxErr := ctx.Err(); ctxErr != nil {
			return PayloadMetadata{}, ctxErr
		}
		return PayloadMetadata{}, fmt.Errorf("read payload metadata: %w", err)
	}
	if err = validateResponse(envelope, id); err != nil {
		c.poison()
		return PayloadMetadata{}, err
	}
	if envelope.Status == "error" {
		return PayloadMetadata{}, &RPCError{Code: envelope.Error.Code, Message: envelope.Error.Message}
	}
	var metadata PayloadMetadata
	if err = json.Unmarshal(envelope.Body, &metadata); err != nil {
		c.poison()
		return PayloadMetadata{}, fmt.Errorf("decode payload metadata: %w", err)
	}
	if metadata.EventID != eventID || metadata.Offset != offset || metadata.Limit != limit || len(metadata.SHA256) != 64 {
		c.poison()
		return PayloadMetadata{}, errors.New("payload metadata is invalid")
	}
	if _, err := hex.DecodeString(metadata.SHA256); err != nil {
		c.poison()
		return PayloadMetadata{}, errors.New("payload metadata hash is invalid")
	}
	expected := uint64(0)
	if offset < metadata.TotalSize {
		expected = metadata.TotalSize - offset
	}
	if limit != 0 && limit < expected {
		expected = limit
	}
	declared, err := protocol.ReadEvidenceHeader(conn)
	if err != nil || declared != expected {
		c.poison()
		if err != nil {
			return PayloadMetadata{}, fmt.Errorf("read payload header: %w", err)
		}
		return PayloadMetadata{}, errors.New("payload length does not match metadata")
	}
	return c.readEvidenceAfterHeader(conn, destination, metadata, expected)
}

func (c *Client) readEvidenceAfterHeader(conn net.Conn, destination io.Writer, metadata PayloadMetadata, expected uint64) (PayloadMetadata, error) {
	if expected > uint64(^uint(0)>>1) {
		c.poison()
		return PayloadMetadata{}, errors.New("payload length exceeds platform limit")
	}
	// This implementation deliberately uses a bounded reader and verifies exactly the declared bytes.
	written, err := io.Copy(destination, io.LimitReader(conn, int64(expected)))
	if err != nil || uint64(written) != expected {
		c.poison()
		if err != nil {
			return PayloadMetadata{}, fmt.Errorf("stream payload: %w", err)
		}
		return PayloadMetadata{}, io.ErrUnexpectedEOF
	}
	metadata.BytesWritten = uint64(written)
	return metadata, nil
}
