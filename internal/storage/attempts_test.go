package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"database/sql"
	"encoding/hex"
	"errors"
	"path/filepath"
	"sync"
	"testing"
	"time"

	"github.com/Jcaltamar/pi-token-smith/migrations"
	_ "modernc.org/sqlite"
)

func TestCaptureAttemptsUpgradeFromRecorded001PreservesEvidence(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "evidence.sqlite")
	db, err := sql.Open("sqlite", path)
	if err != nil {
		t.Fatal(err)
	}
	initial, err := migrations.Files.ReadFile("001_initial.sql")
	if err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, string(initial)); err != nil {
		t.Fatal(err)
	}
	payload := []byte("preexisting evidence")
	hash := sha256.Sum256(payload)
	for _, statement := range []string{
		"INSERT INTO projects(id, created_at) VALUES ('project-1', '2026-01-01T00:00:00Z')",
		"INSERT INTO sessions(id, project_id, created_at) VALUES ('session-1', 'project-1', '2026-01-01T00:00:00Z')",
		"INSERT INTO schema_migrations(version, applied_at) VALUES (1, '2026-01-01T00:00:00Z')",
	} {
		if _, err = db.ExecContext(ctx, statement); err != nil {
			t.Fatal(err)
		}
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO capture_events(id, protocol_version, event_type, project_id, session_id, exchange_id, sequence, occurred_at, received_at, payload_encoding, declared_size, actual_size, payload_sha256) VALUES ('preexisting', 1, 'pi_assistant_message_json', 'project-1', 'session-1', 'exchange-1', 1, '2026-01-01T00:00:00Z', '2026-01-01T00:00:00Z', 'application/json', ?, ?, ?)", len(payload), len(payload), hex.EncodeToString(hash[:])); err != nil {
		t.Fatal(err)
	}
	if _, err = db.ExecContext(ctx, "INSERT INTO capture_event_chunks(event_id, chunk_index, content) VALUES ('preexisting', 0, ?)", payload); err != nil {
		t.Fatal(err)
	}
	if err = db.Close(); err != nil {
		t.Fatal(err)
	}

	store := openStore(t, ctx, path)
	defer store.Close()
	var version int
	if err := store.db.QueryRowContext(ctx, "SELECT version FROM schema_migrations WHERE version = 2").Scan(&version); err != nil || version != 2 {
		t.Fatalf("migration 002 = %d, %v", version, err)
	}
	var got bytes.Buffer
	if _, err := store.ReadEventEvidence(ctx, "preexisting", 0, 0, &got); err != nil || !bytes.Equal(got.Bytes(), payload) {
		t.Fatalf("preserved evidence = %q, %v", got.Bytes(), err)
	}
}

func TestBeginCaptureAttemptIdempotentConflictAndConcurrent(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	attempt := testAttempt("attempt-race", "event-race", 7)
	if err := store.BeginCaptureAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	if err := store.BeginCaptureAttempt(ctx, attempt); err != nil {
		t.Fatalf("idempotent begin: %v", err)
	}
	for _, tt := range []struct {
		name   string
		mutate func(*Attempt)
	}{
		{"sequence", func(attempt *Attempt) { attempt.Sequence++ }},
		{"event type", func(attempt *Attempt) { attempt.EventType = "other_event" }},
		{"protocol version", func(attempt *Attempt) { attempt.ProtocolVersion++ }},
		{"occurred at", func(attempt *Attempt) { attempt.OccurredAt = attempt.OccurredAt.Add(time.Nanosecond) }},
		{"encoding", func(attempt *Attempt) { attempt.Encoding = "application/cbor" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			conflict := attempt
			tt.mutate(&conflict)
			if err := store.BeginCaptureAttempt(ctx, conflict); !errors.Is(err, ErrEventConflict) {
				t.Fatalf("conflicting begin = %v", err)
			}
		})
	}

	concurrent := testAttempt("attempt-concurrent", "event-concurrent", 8)
	var wg sync.WaitGroup
	errs := make(chan error, 2)
	for range 2 {
		wg.Add(1)
		go func() { defer wg.Done(); errs <- store.BeginCaptureAttempt(ctx, concurrent) }()
	}
	wg.Wait()
	close(errs)
	for err := range errs {
		if err != nil {
			t.Fatalf("concurrent begin = %v", err)
		}
	}
}

func TestCaptureAttemptTransitionsAndRecovery(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "evidence.sqlite")
	store := openStore(t, ctx, path)
	pending := testAttempt("attempt-pending", "event-pending", 1)
	if err := store.BeginCaptureAttempt(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCaptureIncomplete(ctx, pending.AttemptID, CaptureFailureAppend); err != nil {
		t.Fatal(err)
	}
	if err := store.MarkCaptureIncomplete(ctx, pending.AttemptID, CaptureFailureAppend); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteCaptureAttempt(ctx, pending.AttemptID); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("completed incomplete = %v", err)
	}
	got, err := store.CaptureAttempt(ctx, pending.AttemptID)
	if err != nil || got.Status != CaptureAttemptIncomplete || got.FailureStage != CaptureFailureAppend {
		t.Fatalf("attempt = %#v, %v", got, err)
	}

	reopen := testAttempt("attempt-reopen", "event-reopen", 2)
	if err := store.BeginCaptureAttempt(ctx, reopen); err != nil {
		t.Fatal(err)
	}
	if err := store.Close(); err != nil {
		t.Fatal(err)
	}
	store = openStore(t, ctx, path)
	defer store.Close()
	result, err := store.RecoverPendingCaptures(ctx)
	if err != nil || result.Incomplete != 1 || result.Completed != 0 {
		t.Fatalf("recovery = %#v, %v", result, err)
	}
	got, err = store.CaptureAttempt(ctx, reopen.AttemptID)
	if err != nil || got.Status != CaptureAttemptIncomplete {
		t.Fatalf("recovered attempt = %#v, %v", got, err)
	}
}

func TestCaptureAttemptMetadataMismatchesNeverComplete(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()

	for _, tt := range []struct {
		name   string
		mutate func(*Event)
	}{
		{"event type", func(event *Event) { event.EventType = "other_event" }},
		{"protocol version", func(event *Event) { event.ProtocolVersion++ }},
		{"occurred at", func(event *Event) { event.OccurredAt = event.OccurredAt.Add(time.Nanosecond) }},
		{"encoding", func(event *Event) { event.Encoding = "application/cbor" }},
	} {
		t.Run(tt.name, func(t *testing.T) {
			attempt := testAttempt("attempt-"+tt.name, "event-"+tt.name, 3)
			if err := store.BeginCaptureAttempt(ctx, attempt); err != nil {
				t.Fatal(err)
			}
			event := eventForAttempt(attempt)
			tt.mutate(&event)
			payload := []byte("complete")
			if _, err := store.AppendEvent(ctx, event, bytes.NewReader(payload), uint64(len(payload))); err != nil {
				t.Fatal(err)
			}
			if err := store.CompleteCaptureAttempt(ctx, attempt.AttemptID); !errors.Is(err, ErrEventConflict) {
				t.Fatalf("complete mismatched attempt = %v", err)
			}
			result, err := store.RecoverPendingCaptures(ctx)
			if err != nil || result.Completed != 0 || result.Incomplete != 1 {
				t.Fatalf("recovery = %#v, %v", result, err)
			}
			got, err := store.CaptureAttempt(ctx, attempt.AttemptID)
			if err != nil || got.Status != CaptureAttemptIncomplete {
				t.Fatalf("attempt = %#v, %v", got, err)
			}
		})
	}
}

func TestCaptureAttemptCanonicalOccurredAtAndSingleConnectionRecovery(t *testing.T) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Second)
	defer cancel()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	attempt := testAttempt("attempt-canonical", "event-canonical", 4)
	attempt.OccurredAt = attempt.OccurredAt.In(time.FixedZone("offset", 2*60*60))
	if err := store.BeginCaptureAttempt(ctx, attempt); err != nil {
		t.Fatal(err)
	}
	zero := attempt
	zero.AttemptID = "attempt-zero"
	zero.OccurredAt = time.Time{}
	if err := store.BeginCaptureAttempt(ctx, zero); !errors.Is(err, ErrEventConflict) {
		t.Fatalf("zero occurrence begin = %v", err)
	}
	got, err := store.CaptureAttempt(ctx, attempt.AttemptID)
	if err != nil || got.OccurredAt.Location() != time.UTC || !got.OccurredAt.Equal(attempt.OccurredAt) {
		t.Fatalf("canonical attempt = %#v, %v", got, err)
	}
	payload := bytes.Repeat([]byte("x"), int(attempt.DeclaredSize))
	if _, err := store.AppendEvent(ctx, eventForAttempt(attempt), bytes.NewReader(payload), attempt.DeclaredSize); err != nil {
		t.Fatal(err)
	}
	if err := store.CompleteCaptureAttempt(ctx, attempt.AttemptID); err != nil {
		t.Fatalf("single-connection completion = %v", err)
	}
	pending := testAttempt("attempt-pending", "event-pending", 5)
	if err := store.BeginCaptureAttempt(ctx, pending); err != nil {
		t.Fatal(err)
	}
	if _, err := store.RecoverPendingCaptures(ctx); err != nil {
		t.Fatalf("single-connection recovery = %v", err)
	}
}

func testAttempt(id, eventID string, sequence int64) Attempt {
	return Attempt{AttemptID: id, EventID: eventID, EventType: "pi_assistant_message_json", ProjectID: "project-1", SessionID: "session-1", ExchangeID: "exchange-1", ProtocolVersion: 1, Sequence: sequence, OccurredAt: time.Date(2026, 1, 1, 12, 0, 0, 123, time.UTC), Encoding: "application/json", DeclaredSize: 8}
}
func eventForAttempt(a Attempt) Event {
	return Event{ID: a.EventID, EventType: a.EventType, ProjectID: a.ProjectID, SessionID: a.SessionID, ExchangeID: a.ExchangeID, ProtocolVersion: a.ProtocolVersion, Sequence: a.Sequence, OccurredAt: a.OccurredAt, Encoding: a.Encoding}
}
