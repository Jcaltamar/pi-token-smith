//go:build linux

package daemon

import (
	"context"
	"database/sql"
	"errors"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/Jcaltamar/pi-token-smith/internal/protocol"
	"github.com/Jcaltamar/pi-token-smith/internal/storage"
	_ "modernc.org/sqlite"
)

func TestCaptureAttemptCompletesAfterNormalCapture(t *testing.T) {
	s := startTestServer(t)
	conn := dialServer(t, s)
	defer conn.Close()
	appendCapture(t, conn, "attempt-complete", []byte("complete"))
	attempt, err := s.store.CaptureAttempt(context.Background(), "attempt-complete")
	if err != nil || attempt.Status != storage.CaptureAttemptCompleted {
		t.Fatalf("attempt = %#v, %v", attempt, err)
	}
}

func TestCaptureFailuresAreIncompleteWithoutEvidenceResidue(t *testing.T) {
	for _, tt := range []struct {
		name        string
		header      uint64
		payload     []byte
		writeHeader bool
	}{
		{"missing header", 0, nil, false},
		{"mismatched header", 3, []byte("body"), true},
		{"truncated payload", 4, []byte("cut"), true},
	} {
		t.Run(tt.name, func(t *testing.T) {
			s := startTestServer(t)
			conn := dialServer(t, s)
			id := "attempt-" + tt.name
			if err := protocol.Encode(conn, request(id, "capture.append", captureBody(id, 4))); err != nil {
				t.Fatal(err)
			}
			if tt.writeHeader {
				if err := protocol.WriteEvidenceHeader(conn, tt.header); err != nil {
					t.Fatal(err)
				}
			}
			if len(tt.payload) != 0 {
				if _, err := conn.Write(tt.payload); err != nil {
					t.Fatal(err)
				}
			}
			_ = conn.Close()
			waitForAttempt(t, s.store, id, storage.CaptureAttemptIncomplete)
			if count, err := s.store.EventCount(context.Background(), id); err != nil || count != 0 {
				t.Fatalf("event count = %d, %v", count, err)
			}
			matches, err := s.store.SearchEvents(context.Background(), "body", 10)
			if err != nil || len(matches) != 0 {
				t.Fatalf("FTS matches = %#v, %v", matches, err)
			}
		})
	}
}

func TestStartRecoversPendingCapturesBeforeHealth(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	store, err := storage.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	attempt := storage.Attempt{AttemptID: "pending-start", EventID: "event-pending-start", EventType: "pi_assistant_message_json", ProjectID: "project", SessionID: "session", ExchangeID: "exchange", ProtocolVersion: protocol.Version, Sequence: 1, OccurredAt: time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC), Encoding: "application/json", DeclaredSize: 4}
	if err := store.BeginCaptureAttempt(context.Background(), attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	s := NewServer(paths)
	if err := s.Start(context.Background()); err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	got, err := s.store.CaptureAttempt(context.Background(), attempt.AttemptID)
	if err != nil || got.Status != storage.CaptureAttemptIncomplete {
		t.Fatalf("recovered attempt = %#v, %v", got, err)
	}
	if health := rpc(t, dialServer(t, s), "system.health", map[string]any{}); health.Status != "ok" {
		t.Fatalf("health = %#v", health)
	}
}

func TestStartFailsCleanlyWhenRecordedMigrationTableIsMissing(t *testing.T) {
	paths := mustRuntimePaths(t, filepath.Join(t.TempDir(), "runtime"))
	store, err := storage.Open(context.Background(), paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	db, err := sql.Open("sqlite", paths.Database)
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.Exec("DROP TABLE capture_attempts"); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	s := NewServer(paths)
	if err := s.Start(context.Background()); err == nil {
		t.Fatal("Start succeeded with missing attempts table")
	}
	if _, err := os.Lstat(paths.Socket); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("failed start left socket: %v", err)
	}
	replacement := NewServer(paths)
	if err := replacement.Start(context.Background()); err == nil || errors.Is(err, ErrAlreadyRunning) {
		t.Fatalf("replacement Start() = %v, want missing-table recovery error without retained lock", err)
	}
}

func waitForAttempt(t *testing.T, store *storage.Store, id string, want storage.CaptureAttemptStatus) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for time.Now().Before(deadline) {
		attempt, err := store.CaptureAttempt(context.Background(), id)
		if err == nil && attempt.Status == want {
			return
		}
		time.Sleep(10 * time.Millisecond)
	}
	t.Fatalf("attempt %q did not reach %q", id, want)
}
