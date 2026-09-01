package storage

import (
	"bytes"
	"context"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"os"
	"path/filepath"
	"strings"
	"strconv"
	"testing"
	"time"
)

func TestOpenMigratesConfiguresAndReopens(t *testing.T) {
	ctx := context.Background()
	path := filepath.Join(t.TempDir(), "evidence.sqlite")
	store := openStore(t, ctx, path)
	defer store.Close()

	assertMode(t, path, 0o600)
	settings, err := store.Settings(ctx)
	if err != nil { t.Fatalf("Settings() error = %v", err) }
	if settings.JournalMode != "wal" || !settings.ForeignKeys || settings.BusyTimeout <= 0 {
		t.Fatalf("Settings() = %#v, want WAL, foreign keys, and busy timeout", settings)
	}
	if err := store.VerifyFTS5(ctx); err != nil { t.Fatalf("VerifyFTS5() error = %v", err) }
	if err := store.Close(); err != nil { t.Fatalf("Close() error = %v", err) }
	if reopened := openStore(t, ctx, path); reopened != nil { defer reopened.Close() }
}

func TestAppendAndReadEvidenceCases(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	cases := []struct { name string; payload []byte }{
		{"empty", nil},
		{"unicode", []byte("hello, 世界")},
		{"multiline", []byte("first\nsecond\nthird\n")},
		{"binary", []byte{0, 0xff, 0x80, '\n'}},
		{"multi chunk", bytes.Repeat([]byte("a"), ChunkSize+17)},
		{"exact boundary", bytes.Repeat([]byte("z"), ChunkSize)},
	}
	for index, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			event := testEvent("event-"+tt.name, int64(index+1))
			result, err := store.AppendEvent(ctx, event, bytes.NewReader(tt.payload), uint64(len(tt.payload)))
			if err != nil { t.Fatalf("AppendEvent() error = %v", err) }
			wantHash := sha256.Sum256(tt.payload)
			if result.ActualSize != uint64(len(tt.payload)) || result.SHA256 != hex.EncodeToString(wantHash[:]) || result.AlreadyPresent {
				t.Fatalf("AppendEvent() = %#v", result)
			}
			var got bytes.Buffer
			read, err := store.ReadEventEvidence(ctx, event.ID, 0, 0, &got)
			if err != nil { t.Fatalf("ReadEventEvidence() error = %v", err) }
			if !bytes.Equal(got.Bytes(), tt.payload) || read.TotalSize != uint64(len(tt.payload)) || read.BytesWritten != uint64(len(tt.payload)) {
				t.Fatalf("read = %#v payload=%x, want %x", read, got.Bytes(), tt.payload)
			}
		})
	}
}

func TestAppendStreamsBoundedReadsAndPaginates(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	payload := bytes.Repeat([]byte("0123456789"), ChunkSize)
	reader := &boundedReader{reader: bytes.NewReader(payload), max: ChunkSize}
	if _, err := store.AppendEvent(ctx, testEvent("streamed", 1), reader, uint64(len(payload))); err != nil { t.Fatalf("AppendEvent() error = %v", err) }
	if reader.largest > ChunkSize { t.Fatalf("largest requested read = %d, want <= %d", reader.largest, ChunkSize) }

	var got bytes.Buffer
	result, err := store.ReadEventEvidence(ctx, "streamed", 7, 19, &got)
	if err != nil { t.Fatalf("ReadEventEvidence() error = %v", err) }
	if want := payload[7:26]; !bytes.Equal(got.Bytes(), want) || result.BytesWritten != uint64(len(want)) { t.Fatalf("page = %q %#v, want %q", got.Bytes(), result, want) }
}

func TestAppendRejectsTruncationAndRollsBack(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	id := "invalid-truncated"
	_, err := store.AppendEvent(ctx, testEvent(id, 1), bytes.NewReader([]byte("short")), 6)
	if !errors.Is(err, io.ErrUnexpectedEOF) { t.Fatalf("AppendEvent() error = %v, want io.ErrUnexpectedEOF", err) }
	assertMissing(t, ctx, store, id)
	var chunks, indexed int
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM capture_event_chunks WHERE event_id = ?", id).Scan(&chunks); err != nil { t.Fatalf("count chunks: %v", err) }
	if err := store.db.QueryRowContext(ctx, "SELECT count(*) FROM capture_fts WHERE event_id = ?", id).Scan(&indexed); err != nil { t.Fatalf("count FTS rows: %v", err) }
	if chunks != 0 || indexed != 0 { t.Fatalf("rollback residue: chunks=%d FTS=%d", chunks, indexed) }
}

func TestAppendConsumesOnlyDeclaredBytes(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	reader := &neverEOFFrameReader{body: []byte("body"), following: []byte("next-frame")}
	if _, err := store.AppendEvent(ctx, testEvent("bounded-frame", 1), reader, 4); err != nil { t.Fatalf("AppendEvent() error = %v", err) }
	if reader.reads != 1 || !bytes.Equal(reader.following, []byte("next-frame")) { t.Fatalf("reader reads=%d following=%q, want one read and untouched following frame", reader.reads, reader.following) }
}

func TestDuplicateValidatesStoredChunkTruth(t *testing.T) {
	ctx := context.Background()
	payload := bytes.Repeat([]byte("x"), ChunkSize+1)
	for _, corrupt := range []struct { name, statement string }{
		{"corrupt content", "UPDATE capture_event_chunks SET content = X'00' WHERE event_id = ? AND chunk_index = 0"},
		{"missing chunk", "DELETE FROM capture_event_chunks WHERE event_id = ? AND chunk_index = 1"},
	} {
		t.Run(corrupt.name, func(t *testing.T) {
			store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
			defer store.Close()
			id := "duplicate-" + strings.ReplaceAll(corrupt.name, " ", "-")
			if _, err := store.AppendEvent(ctx, testEvent(id, 1), bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatal(err) }
			dropEvidenceTriggers(t, ctx, store)
			if _, err := store.db.ExecContext(ctx, corrupt.statement, id); err != nil { t.Fatalf("corrupt test evidence: %v", err) }
			if _, err := store.AppendEvent(ctx, testEvent(id, 1), bytes.NewReader(payload), uint64(len(payload))); !errors.Is(err, ErrEventConflict) { t.Fatalf("duplicate error = %v, want ErrEventConflict", err) }
		})
	}
}

func TestEvidenceTablesAreImmutable(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	payload := []byte("immutable")
	if _, err := store.AppendEvent(ctx, testEvent("immutable", 1), bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatal(err) }
	for _, statement := range []string{
		"UPDATE capture_events SET payload_sha256 = 'changed' WHERE id = 'immutable'",
		"DELETE FROM capture_events WHERE id = 'immutable'",
		"UPDATE capture_event_chunks SET content = X'00' WHERE event_id = 'immutable'",
		"DELETE FROM capture_event_chunks WHERE event_id = 'immutable'",
	} { if _, err := store.db.ExecContext(ctx, statement); err == nil { t.Fatalf("%q succeeded, want immutable-table failure", statement) } }
	var got bytes.Buffer
	if err := readInto(ctx, store, "immutable", 0, 0, &got); err != nil || !bytes.Equal(got.Bytes(), payload) { t.Fatalf("stored bytes = %q, %v; want %q", got.Bytes(), err, payload) }
}

func TestReadEvidencePaginationAndCorruption(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	payload := bytes.Repeat([]byte("a"), ChunkSize*3+7)
	if _, err := store.AppendEvent(ctx, testEvent("pages", 1), bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatal(err) }
	for _, tt := range []struct { name string; offset, limit uint64; want []byte }{
		{"zero limit reads remaining", 5, 0, payload[5:]},
		{"single chunk page", ChunkSize + 3, 9, payload[ChunkSize+3 : ChunkSize+12]},
		{"at eof", uint64(len(payload)), 1, nil},
		{"past eof", uint64(len(payload)) + 1, 1, nil},
	} { t.Run(tt.name, func(t *testing.T) { var got bytes.Buffer; result, err := store.ReadEventEvidence(ctx, "pages", tt.offset, tt.limit, &got); if err != nil || !bytes.Equal(got.Bytes(), tt.want) || result.BytesWritten != uint64(len(tt.want)) { t.Fatalf("ReadEventEvidence() = %q, %#v, %v; want %q", got.Bytes(), result, err, tt.want) } }) }
	if _, err := store.ReadEventEvidence(ctx, "missing", 0, 1, io.Discard); !errors.Is(err, ErrEventNotFound) { t.Fatalf("missing event error = %v", err) }
	cancelled, cancel := context.WithCancel(ctx); cancel()
	if _, err := store.ReadEventEvidence(cancelled, "pages", 0, 1, io.Discard); !errors.Is(err, context.Canceled) { t.Fatalf("cancelled read error = %v", err) }
	if _, err := store.ReadEventEvidence(ctx, "pages", 0, 1, errWriter{}); !errors.Is(err, errWriterFailure) { t.Fatalf("writer failure = %v", err) }
	dropEvidenceTriggers(t, ctx, store)
	if _, err := store.db.ExecContext(ctx, "UPDATE capture_event_chunks SET content = X'00' WHERE event_id = 'pages' AND chunk_index = 1"); err != nil { t.Fatal(err) }
	if _, err := store.ReadEventEvidence(ctx, "pages", ChunkSize, 1, io.Discard); !errors.Is(err, ErrEventConflict) { t.Fatalf("corrupt chunk error = %v, want ErrEventConflict", err) }
	if _, err := store.db.ExecContext(ctx, "UPDATE capture_event_chunks SET content = X'00' WHERE event_id = 'pages' AND chunk_index = 0"); err != nil { t.Fatal(err) }
	var page bytes.Buffer
	if _, err := store.ReadEventEvidence(ctx, "pages", ChunkSize*2, 1, &page); err != nil || !bytes.Equal(page.Bytes(), payload[ChunkSize*2:ChunkSize*2+1]) { t.Fatalf("direct chunk page = %q, %v; want %q", page.Bytes(), err, payload[ChunkSize*2:ChunkSize*2+1]) }
	if _, err := store.db.ExecContext(ctx, "DELETE FROM capture_event_chunks WHERE event_id = 'pages' AND chunk_index = 2"); err != nil { t.Fatal(err) }
	if _, err := store.ReadEventEvidence(ctx, "pages", ChunkSize*2, 1, io.Discard); !errors.Is(err, ErrEventConflict) { t.Fatalf("chunk gap error = %v, want ErrEventConflict", err) }
	maxSQLiteInt := int64(^uint64(0) >> 1)
	if _, err := store.db.ExecContext(ctx, "UPDATE capture_events SET actual_size = ? WHERE id = 'pages'", maxSQLiteInt); err != nil { t.Fatal(err) }
	if _, err := store.ReadEventEvidence(ctx, "pages", uint64(maxSQLiteInt-1), 2, io.Discard); !errors.Is(err, ErrEventConflict) { t.Fatalf("large offset error = %v, want ErrEventConflict", err) }
}

func TestAppendIsIdempotentAndRejectsConflictingID(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	event := testEvent("same-id", 1)
	payload := []byte("identical evidence")
	if _, err := store.AppendEvent(ctx, event, bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatalf("first AppendEvent() error = %v", err) }
	duplicate, err := store.AppendEvent(ctx, event, bytes.NewReader(payload), uint64(len(payload)))
	if err != nil || !duplicate.AlreadyPresent { t.Fatalf("duplicate AppendEvent() = %#v, %v", duplicate, err) }
	_, err = store.AppendEvent(ctx, event, bytes.NewReader([]byte("different evidence")), uint64(len("different evidence")))
	if !errors.Is(err, ErrEventConflict) { t.Fatalf("conflicting AppendEvent() error = %v, want ErrEventConflict", err) }
	assertEventCount(t, ctx, store, "same-id", 1)
}

func TestEventReferenceMarshalsSnakeCaseJSON(t *testing.T) {
	got, err := json.Marshal(EventReference{ID: "event-1", ProjectID: "project-1", SessionID: "session-1", ExchangeID: "exchange-1", Sequence: 42})
	if err != nil { t.Fatalf("Marshal(EventReference) error = %v", err) }
	const want = `{"id":"event-1","project_id":"project-1","session_id":"session-1","exchange_id":"exchange-1","sequence":42}`
	if string(got) != want { t.Fatalf("Marshal(EventReference) = %s, want %s", got, want) }
}

func TestSearchIndexesOnlyValidUTF8Chunks(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	if _, err := store.AppendEvent(ctx, testEvent("searchable", 1), strings.NewReader("alpha needle omega"), uint64(len("alpha needle omega"))); err != nil { t.Fatal(err) }
	if _, err := store.AppendEvent(ctx, testEvent("binary", 2), bytes.NewReader([]byte{0xff, ' ', 'n', 'e', 'e', 'd', 'l', 'e'}), 8); err != nil { t.Fatal(err) }
	matches, err := store.SearchEvents(ctx, "needle", 10)
	if err != nil { t.Fatalf("SearchEvents() error = %v", err) }
	if len(matches) != 1 || matches[0].ID != "searchable" { t.Fatalf("SearchEvents() = %#v, want searchable only", matches) }
}

func TestSearchHonorsPositiveLimit(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	for i := 0; i < 5; i++ {
		id := "limited-" + strconv.Itoa(i)
		payload := []byte("bounded needle")
		if _, err := store.AppendEvent(ctx, testEvent(id, int64(i)), bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatal(err) }
	}
	matches, err := store.SearchEvents(ctx, "needle", 2)
	if err != nil || len(matches) != 2 { t.Fatalf("SearchEvents() = %d matches, %v; want 2, nil", len(matches), err) }
	if _, err := store.SearchEvents(ctx, "needle", 0); err == nil { t.Fatal("SearchEvents() with zero limit succeeded") }
}

func TestSearchDoesNotJoinTextAcrossChunkBoundaries(t *testing.T) {
	ctx := context.Background()
	store := openStore(t, ctx, filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	payload := []byte(strings.Repeat("x", ChunkSize-1) + "needle")
	if _, err := store.AppendEvent(ctx, testEvent("split-token", 1), bytes.NewReader(payload), uint64(len(payload))); err != nil { t.Fatal(err) }
	matches, err := store.SearchEvents(ctx, "needle", 10)
	if err != nil { t.Fatalf("SearchEvents() error = %v", err) }
	if len(matches) != 0 { t.Fatalf("SearchEvents() = %#v, want no cross-chunk match", matches) }
}

func TestAppendHonorsCanceledContext(t *testing.T) {
	store := openStore(t, context.Background(), filepath.Join(t.TempDir(), "evidence.sqlite"))
	defer store.Close()
	ctx, cancel := context.WithCancel(context.Background())
	cancel()
	_, err := store.AppendEvent(ctx, testEvent("cancelled", 1), strings.NewReader("body"), 4)
	if !errors.Is(err, context.Canceled) { t.Fatalf("AppendEvent() error = %v, want context.Canceled", err) }
	assertMissing(t, context.Background(), store, "cancelled")
}

var errWriterFailure = errors.New("writer failure")

type errWriter struct{}
func (errWriter) Write([]byte) (int, error) { return 0, errWriterFailure }

type neverEOFFrameReader struct { body, following []byte; reads int }
func (r *neverEOFFrameReader) Read(p []byte) (int, error) {
	r.reads++
	if r.reads > 1 { panic("AppendEvent read beyond declared frame") }
	n := copy(p, r.body)
	r.body = r.body[n:]
	return n, nil
}

func dropEvidenceTriggers(t *testing.T, ctx context.Context, store *Store) {
	t.Helper()
	for _, name := range []string{"capture_events_no_update", "capture_events_no_delete", "capture_event_chunks_no_update", "capture_event_chunks_no_delete"} {
		if _, err := store.db.ExecContext(ctx, "DROP TRIGGER "+name); err != nil { t.Fatalf("drop %s: %v", name, err) }
	}
}

func readInto(ctx context.Context, store *Store, id string, offset, limit uint64, destination io.Writer) error {
	_, err := store.ReadEventEvidence(ctx, id, offset, limit, destination)
	return err
}

func openStore(t *testing.T, ctx context.Context, path string) *Store { t.Helper(); s, err := Open(ctx, path); if err != nil { t.Fatalf("Open() error = %v", err) }; return s }
func testEvent(id string, sequence int64) Event { return Event{ID:id, ProtocolVersion:1, EventType:"pi_assistant_message_json", ProjectID:"project-1", SessionID:"session-1", ExchangeID:"exchange-1", Sequence:sequence, OccurredAt:time.Date(2026,1,2,3,4,5,0,time.UTC), Encoding:"application/json"} }
func assertMissing(t *testing.T, ctx context.Context, s *Store, id string) { t.Helper(); var got bytes.Buffer; _, err := s.ReadEventEvidence(ctx, id, 0, 0, &got); if !errors.Is(err, ErrEventNotFound) { t.Fatalf("event %q read error = %v, want ErrEventNotFound", id, err) } }
func assertEventCount(t *testing.T, ctx context.Context, s *Store, id string, want int) { t.Helper(); got, err := s.EventCount(ctx, id); if err != nil || got != want { t.Fatalf("EventCount() = %d, %v; want %d, nil", got, err, want) } }
func assertMode(t *testing.T, path string, want os.FileMode) { t.Helper(); info, err := os.Stat(path); if err != nil { t.Fatal(err) }; if got:=info.Mode().Perm(); got != want { t.Fatalf("mode=%04o want=%04o",got,want) } }

type boundedReader struct { reader io.Reader; max, largest int }
func (r *boundedReader) Read(p []byte) (int,error) { if len(p)>r.largest {r.largest=len(p)}; if len(p)>r.max { return 0, errors.New("read buffer exceeds bound") }; return r.reader.Read(p) }
