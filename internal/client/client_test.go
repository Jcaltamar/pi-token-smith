//go:build linux

package client

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/daemon"
	"github.com/Jcaltamar/pi-token-smith/internal/protocol"
)

func TestClientSerializesConcurrentRequestsAndStreamsPayload(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	payload := []byte("exact evidence bytes")
	sum := sha256.Sum256(payload)
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		for {
			var request protocol.Request
			if protocol.Decode(conn, &request) != nil {
				return
			}
			var body any = map[string]any{"version": protocol.Version, "status": "healthy"}
			if request.Operation == "search.events" {
				body = map[string]any{"events": []map[string]any{{"id": "event-1", "project_id": "project", "session_id": "session", "exchange_id": "exchange", "sequence": 1}}}
			}
			if request.Operation == "event.read_payload" {
				body = map[string]any{"event_id": "event-1", "total_size": len(payload), "sha256": hex.EncodeToString(sum[:]), "offset": 0, "limit": 0}
			}
			raw, _ := json.Marshal(body)
			if protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: request.RequestID, Status: "ok", Body: raw}) != nil {
				return
			}
			if request.Operation == "event.read_payload" {
				if _, err := protocol.WriteEvidence(conn, uint64(len(payload)), bytes.NewReader(payload)); err != nil {
					return
				}
			}
		}
	}()
	c := New(socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer c.Close()
	var wg sync.WaitGroup
	for range 8 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			if _, err := c.Health(context.Background()); err != nil {
				t.Errorf("Health() = %v", err)
			}
		}()
	}
	wg.Wait()
	matches, err := c.Search(context.Background(), "evidence", 10)
	if err != nil || len(matches) != 1 || matches[0].ID != "event-1" {
		t.Fatalf("Search() = %#v, %v", matches, err)
	}
	var got bytes.Buffer
	metadata, err := c.ReadPayload(context.Background(), "event-1", 0, 0, &got)
	if err != nil || !bytes.Equal(got.Bytes(), payload) || metadata.BytesWritten != uint64(len(payload)) {
		t.Fatalf("ReadPayload() = %#v, %q, %v", metadata, got.Bytes(), err)
	}
}

func TestClientNilContextUsesBackgroundForCallAndPayload(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	payload := []byte("nil context payload")
	sum := sha256.Sum256(payload)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		for range 2 {
			var request protocol.Request
			if protocol.Decode(conn, &request) != nil {
				return
			}
			var body any = map[string]any{"version": protocol.Version, "status": "healthy"}
			if request.Operation == "event.read_payload" {
				body = map[string]any{"event_id": "event", "total_size": len(payload), "sha256": hex.EncodeToString(sum[:]), "offset": 0, "limit": 0}
			}
			raw, _ := json.Marshal(body)
			if protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: request.RequestID, Status: "ok", Body: raw}) != nil {
				return
			}
			if request.Operation == "event.read_payload" {
				_, _ = protocol.WriteEvidence(conn, uint64(len(payload)), bytes.NewReader(payload))
			}
		}
	}()
	c := New(socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer c.Close()
	if health, healthErr := c.Health(nil); healthErr != nil || health.Status != "healthy" {
		t.Fatalf("Health(nil) = %#v, %v", health, healthErr)
	}
	var got bytes.Buffer
	metadata, readErr := c.ReadPayload(nil, "event", 0, 0, &got)
	if readErr != nil || !bytes.Equal(got.Bytes(), payload) || metadata.BytesWritten != uint64(len(payload)) {
		t.Fatalf("ReadPayload(nil) = %#v, %q, %v", metadata, got.Bytes(), readErr)
	}
}

func TestClientReadPayloadAcceptsOffsetAtOrPastEOF(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	payload := []byte("evidence")
	sum := sha256.Sum256(payload)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		for range 2 {
			var request protocol.Request
			if protocol.Decode(conn, &request) != nil {
				return
			}
			var body struct {
				EventID string `json:"event_id"`
				Offset  uint64 `json:"offset"`
				Limit   uint64 `json:"limit"`
			}
			_ = json.Unmarshal(request.Body, &body)
			raw, _ := json.Marshal(map[string]any{"event_id": body.EventID, "total_size": len(payload), "sha256": hex.EncodeToString(sum[:]), "offset": body.Offset, "limit": body.Limit})
			_ = protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: request.RequestID, Status: "ok", Body: raw})
			_ = protocol.WriteEvidenceHeader(conn, 0)
		}
	}()
	c := New(socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer c.Close()
	for _, offset := range []uint64{uint64(len(payload)), uint64(len(payload) + 1)} {
		var got bytes.Buffer
		metadata, readErr := c.ReadPayload(context.Background(), "event", offset, 0, &got)
		if readErr != nil || metadata.BytesWritten != 0 || got.Len() != 0 {
			t.Fatalf("ReadPayload(offset=%d) = %#v, %q, %v", offset, metadata, got.Bytes(), readErr)
		}
	}
}

func TestClientPayloadFailuresForceFreshConnection(t *testing.T) {
	for _, tt := range []struct {
		name        string
		frameLength uint64
		truncate    bool
		writer      io.Writer
	}{
		{name: "mismatched evidence header", frameLength: 2, writer: io.Discard},
		{name: "truncated evidence frame", frameLength: 3, truncate: true, writer: io.Discard},
		{name: "destination writer failure", frameLength: 3, writer: failingWriter{}},
	} {
		t.Run(tt.name, func(t *testing.T) {
			socket := filepath.Join(t.TempDir(), "daemon.sock")
			listener, err := net.Listen("unix", socket)
			if err != nil {
				t.Fatal(err)
			}
			defer listener.Close()
			payload := []byte("abc")
			sum := sha256.Sum256(payload)
			accepted := make(chan struct{}, 2)
			go func() {
				for range 2 {
					conn, acceptErr := listener.Accept()
					if acceptErr != nil {
						return
					}
					accepted <- struct{}{}
					var request protocol.Request
					if protocol.Decode(conn, &request) != nil {
						_ = conn.Close()
						continue
					}
					if request.Operation == "event.read_payload" {
						raw, _ := json.Marshal(map[string]any{"event_id": "event", "total_size": len(payload), "sha256": hex.EncodeToString(sum[:]), "offset": 0, "limit": 0})
						_ = protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: request.RequestID, Status: "ok", Body: raw})
						if tt.truncate {
							_ = protocol.WriteEvidenceHeader(conn, tt.frameLength)
							_, _ = conn.Write(payload[:len(payload)-1])
						} else {
							_, _ = protocol.WriteEvidence(conn, tt.frameLength, bytes.NewReader(payload))
						}
					} else {
						raw, _ := json.Marshal(map[string]any{"version": protocol.Version, "status": "healthy"})
						_ = protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: request.RequestID, Status: "ok", Body: raw})
					}
					_ = conn.Close()
				}
			}()
			c := New(socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
			defer c.Close()
			if _, err := c.ReadPayload(context.Background(), "event", 0, 0, tt.writer); err == nil {
				t.Fatal("ReadPayload succeeded")
			}
			if _, err := c.Health(context.Background()); err != nil {
				t.Fatalf("Health after poisoned payload = %v", err)
			}
			if len(accepted) != 2 {
				t.Fatalf("connections accepted = %d, want 2", len(accepted))
			}
		})
	}
}

type failingWriter struct{}

func (failingWriter) Write([]byte) (int, error) { return 0, errors.New("destination failed") }

func TestClientContextCancellationInterruptsGateAndSocketIO(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	requestRead := make(chan struct{})
	releaseServer := make(chan struct{})
	defer close(releaseServer)
	go func() {
		conn, acceptErr := listener.Accept()
		if acceptErr != nil {
			return
		}
		defer conn.Close()
		var request protocol.Request
		if protocol.Decode(conn, &request) == nil {
			close(requestRead)
			<-releaseServer
		}
	}()
	c := New(socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer c.Close()
	ioCtx, cancelIO := context.WithCancel(context.Background())
	ioResult := make(chan error, 1)
	go func() { _, callErr := c.Health(ioCtx); ioResult <- callErr }()
	select {
	case <-requestRead:
	case <-time.After(time.Second):
		t.Fatal("server did not receive first request")
	}
	gateCtx, cancelGate := context.WithCancel(context.Background())
	cancelGate()
	if _, err := c.Health(gateCtx); !errors.Is(err, context.Canceled) {
		t.Fatalf("gate wait = %v, want context cancellation", err)
	}
	cancelIO()
	select {
	case err := <-ioResult:
		if !errors.Is(err, context.Canceled) {
			t.Fatalf("socket I/O = %v, want context cancellation", err)
		}
	case <-time.After(time.Second):
		t.Fatal("context cancellation did not interrupt socket I/O")
	}
}

func TestClientIntegrationWithDaemonAndCancellationCleanup(t *testing.T) {
	paths, err := daemon.NewRuntimePaths(filepath.Join(t.TempDir(), "runtime"))
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	server := daemon.NewServer(paths)
	if err := server.Start(ctx); err != nil {
		t.Fatal(err)
	}
	captureClient, err := net.Dial("unix", paths.Socket)
	if err != nil {
		t.Fatal(err)
	}
	payload := []byte("exact daemon evidence")
	appendDaemonCapture(t, captureClient, "event", payload)
	_ = captureClient.Close()

	c := New(paths.Socket, Options{DialTimeout: time.Second, ReadTimeout: time.Second, WriteTimeout: time.Second})
	defer c.Close()
	health, err := c.Health(context.Background())
	if err != nil || health.Status != "healthy" {
		t.Fatalf("Health() = %#v, %v", health, err)
	}
	events, err := c.Search(context.Background(), "exact", 10)
	if err != nil || len(events) != 1 || events[0].ID != "event" {
		t.Fatalf("Search() = %#v, %v", events, err)
	}
	var got bytes.Buffer
	metadata, err := c.ReadPayload(context.Background(), "event", 6, 5, &got)
	if err != nil || !bytes.Equal(got.Bytes(), payload[6:11]) || metadata.BytesWritten != 5 {
		t.Fatalf("ReadPayload() = %#v, %q, %v", metadata, got.Bytes(), err)
	}
	cancel()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		if _, err := os.Lstat(paths.Socket); errors.Is(err, os.ErrNotExist) {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatal("cancelled daemon left socket")
}

func appendDaemonCapture(t *testing.T, conn net.Conn, id string, payload []byte) {
	t.Helper()
	body := map[string]any{"event_id": id, "event_type": "pi_assistant_message_json", "project_id": "project", "session_id": "session", "exchange_id": "exchange", "sequence": 1, "occurred_at": "2026-01-02T03:04:05Z", "encoding": "application/json", "payload_size": len(payload)}
	raw, _ := json.Marshal(body)
	if err := protocol.Encode(conn, protocol.Request{ProtocolVersion: protocol.Version, RequestID: "capture", Operation: "capture.append", SentAt: time.Now().UTC(), Body: raw}); err != nil {
		t.Fatal(err)
	}
	if _, err := protocol.WriteEvidence(conn, uint64(len(payload)), bytes.NewReader(payload)); err != nil {
		t.Fatal(err)
	}
	var response protocol.Response
	if err := protocol.Decode(conn, &response); err != nil || response.Status != "ok" {
		t.Fatalf("capture = %#v, %v", response, err)
	}
}

func TestClientRejectsWrongResponseIDAndPoisonsConnection(t *testing.T) {
	socket := filepath.Join(t.TempDir(), "daemon.sock")
	listener, err := net.Listen("unix", socket)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	go func() {
		conn, err := listener.Accept()
		if err != nil {
			return
		}
		defer conn.Close()
		var request protocol.Request
		if protocol.Decode(conn, &request) == nil {
			_ = protocol.Encode(conn, protocol.Response{ProtocolVersion: protocol.Version, RequestID: "wrong", Status: "ok", Body: json.RawMessage(`{}`)})
		}
	}()
	c := New(socket, Options{ReadTimeout: 20 * time.Millisecond})
	defer c.Close()
	if _, err := c.Health(context.Background()); err == nil {
		t.Fatal("Health accepted mismatched response ID")
	}
	if _, err := c.Health(context.Background()); err == nil {
		t.Fatal("Health reused poisoned connection")
	}
}
