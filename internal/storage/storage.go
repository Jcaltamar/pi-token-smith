// Package storage provides the daemon-owned SQLite evidence store.
package storage

import (
	"context"
	"crypto/sha256"
	"database/sql"
	"embed"
	"encoding/hex"
	"errors"
	"fmt"
	"io"
	"io/fs"
	"os"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"time"
	"unicode/utf8"

	"github.com/Jcaltamar/pi-token-smith/migrations"
	_ "modernc.org/sqlite"
)

// ChunkSize is the bounded evidence chunk size used for ingestion and retrieval.
const ChunkSize = 32 << 10

var (
	ErrEventNotFound = errors.New("storage event not found")
	ErrEventConflict = errors.New("storage event ID conflicts with existing immutable evidence")
)

// Event identifies immutable evidence and its capture metadata.
type Event struct {
	ID, EventType, ProjectID, SessionID, ExchangeID, Encoding string
	ProtocolVersion int
	Sequence        int64
	OccurredAt      time.Time
}

// AppendResult describes the integrity metadata persisted for one event.
type AppendResult struct {
	AlreadyPresent bool
	ActualSize     uint64
	SHA256         string
}

// ReadResult describes a paginated evidence read. A limit of zero reads all bytes after offset.
type ReadResult struct { TotalSize, BytesWritten uint64; SHA256 string }

// EventReference is a searchable event identity. FTS results are references only.
type EventReference struct { ID, ProjectID, SessionID, ExchangeID string; Sequence int64 }

// Settings reports effective connection settings verified at Open.
type Settings struct { JournalMode string; ForeignKeys bool; BusyTimeout time.Duration }

// Store owns a single SQLite connection for the daemon process.
type Store struct { db *sql.DB; settings Settings }

// Open creates or opens path, protects it on Linux, applies embedded migrations, and verifies FTS5.
func Open(ctx context.Context, path string) (*Store, error) {
	if path == "" { return nil, errors.New("storage database path is empty") }
	if err := os.MkdirAll(filepath.Dir(path), 0o700); err != nil { return nil, fmt.Errorf("create database directory: %w", err) }
	file, err := os.OpenFile(path, os.O_RDWR|os.O_CREATE, 0o600)
	if err != nil { return nil, fmt.Errorf("create database file: %w", err) }
	if err := file.Close(); err != nil { return nil, fmt.Errorf("close database file: %w", err) }
	if err := protectDatabase(path); err != nil { return nil, err }
	db, err := sql.Open("sqlite", path)
	if err != nil { return nil, fmt.Errorf("open sqlite database: %w", err) }
	db.SetMaxOpenConns(1)
	db.SetMaxIdleConns(1)
	fail := func(err error) (*Store, error) { _ = db.Close(); return nil, err }
	settings, err := configure(ctx, db)
	if err != nil { return fail(err) }
	if err := migrate(ctx, db, migrations.Files); err != nil { return fail(err) }
	store := &Store{db: db, settings: settings}
	if err := store.VerifyFTS5(ctx); err != nil { return fail(err) }
	return store, nil
}

// Close releases the SQLite connection.
func (s *Store) Close() error { if s == nil || s.db == nil { return nil }; return s.db.Close() }

// Settings returns the effective SQLite connection settings.
func (s *Store) Settings(context.Context) (Settings, error) { return s.settings, nil }

// VerifyFTS5 proves FTS5 is available with a real virtual table insert and MATCH query.
func (s *Store) VerifyFTS5(ctx context.Context) error {
	tx, err := s.db.BeginTx(ctx, nil); if err != nil { return err }; defer tx.Rollback()
	if _, err = tx.ExecContext(ctx, "DROP TABLE IF EXISTS temp.capture_fts_probe"); err != nil { return fmt.Errorf("clear FTS5 probe: %w", err) }
	if _, err = tx.ExecContext(ctx, "CREATE VIRTUAL TABLE temp.capture_fts_probe USING fts5(content)"); err != nil { return fmt.Errorf("FTS5 unavailable: %w", err) }
	if _, err = tx.ExecContext(ctx, "INSERT INTO temp.capture_fts_probe(content) VALUES ('fts5-probe-token')"); err != nil { return fmt.Errorf("FTS5 probe insert: %w", err) }
	var count int
	if err = tx.QueryRowContext(ctx, "SELECT count(*) FROM temp.capture_fts_probe WHERE capture_fts_probe MATCH '\"fts5-probe-token\"'").Scan(&count); err != nil { return fmt.Errorf("FTS5 probe match: %w", err) }
	if count != 1 { return fmt.Errorf("FTS5 probe match count = %d, want 1", count) }
	return tx.Commit()
}

// AppendEvent streams exactly declaredSize bytes into immutable chunk rows. It returns immediately after those bytes are consumed and leaves any following bytes unread; callers with framed protocols must validate frame boundaries themselves. It never binds full evidence.
func (s *Store) AppendEvent(ctx context.Context, event Event, evidence io.Reader, declaredSize uint64) (AppendResult, error) {
	if err := ctx.Err(); err != nil { return AppendResult{}, err }
	if evidence == nil { return AppendResult{}, errors.New("storage evidence reader is nil") }
	if declaredSize > uint64(^uint(0)>>1) { return AppendResult{}, fmt.Errorf("declared evidence size exceeds platform limit") }
	tx, err := s.db.BeginTx(ctx, nil); if err != nil { return AppendResult{}, err }; defer tx.Rollback()
	var existingDeclared, existingSize uint64; var existingHash string
	err = tx.QueryRowContext(ctx, "SELECT declared_size, actual_size, payload_sha256 FROM capture_events WHERE id = ?", event.ID).Scan(&existingDeclared, &existingSize, &existingHash)
	if err == nil {
		size, hash, streamErr := consumeEvidence(ctx, evidence, declaredSize, nil)
		if streamErr != nil { return AppendResult{}, streamErr }
		storedSize, storedHash, integrityErr := storedEvidence(ctx, tx, event.ID, existingDeclared)
		if integrityErr != nil || storedSize != existingSize || storedHash != existingHash || size != storedSize || hash != storedHash { return AppendResult{}, ErrEventConflict }
		return AppendResult{AlreadyPresent:true, ActualSize:size, SHA256:hash}, nil
	}
	if !errors.Is(err, sql.ErrNoRows) { return AppendResult{}, err }
	now := time.Now().UTC().Format(time.RFC3339Nano)
	occurred := event.OccurredAt.UTC().Format(time.RFC3339Nano)
	if event.OccurredAt.IsZero() { occurred = now }
	if _, err = tx.ExecContext(ctx, "INSERT INTO projects(id, created_at) VALUES (?, ?) ON CONFLICT(id) DO NOTHING", event.ProjectID, now); err != nil { return AppendResult{}, err }
	if _, err = tx.ExecContext(ctx, "INSERT INTO sessions(id, project_id, created_at) VALUES (?, ?, ?) ON CONFLICT(id) DO NOTHING", event.SessionID, event.ProjectID, now); err != nil { return AppendResult{}, err }
	if _, err = tx.ExecContext(ctx, `INSERT INTO capture_events (id, protocol_version, event_type, project_id, session_id, exchange_id, sequence, occurred_at, received_at, payload_encoding, declared_size, actual_size, payload_sha256) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, 0, '')`, event.ID, event.ProtocolVersion, event.EventType, event.ProjectID, event.SessionID, nullable(event.ExchangeID), event.Sequence, occurred, now, event.Encoding, declaredSize); err != nil { return AppendResult{}, err }
	chunkIndex := 0
	size, hash, err := consumeEvidence(ctx, evidence, declaredSize, func(chunk []byte) error {
		if _, err := tx.ExecContext(ctx, "INSERT INTO capture_event_chunks(event_id, chunk_index, content) VALUES (?, ?, ?)", event.ID, chunkIndex, chunk); err != nil { return err }
		if utf8.Valid(chunk) { if _, err := tx.ExecContext(ctx, "INSERT INTO capture_fts(event_id, project_id, session_id, exchange_id, content) VALUES (?, ?, ?, ?, ?)", event.ID, event.ProjectID, event.SessionID, nullable(event.ExchangeID), string(chunk)); err != nil { return err } }
		chunkIndex++; return nil
	})
	if err != nil { return AppendResult{}, err }
	if _, err = tx.ExecContext(ctx, "UPDATE capture_events SET actual_size = ?, payload_sha256 = ? WHERE id = ?", size, hash, event.ID); err != nil { return AppendResult{}, err }
	if err = tx.Commit(); err != nil { return AppendResult{}, err }
	return AppendResult{ActualSize:size, SHA256:hash}, nil
}

// ReadEventEvidence streams exact stored bytes to destination. It seeks directly to the first chunk intersecting offset and does not load the full evidence. A limit of zero reads all remaining bytes.
func (s *Store) ReadEventEvidence(ctx context.Context, eventID string, offset, limit uint64, destination io.Writer) (ReadResult, error) {
	if err := ctx.Err(); err != nil { return ReadResult{}, err }
	if destination == nil { return ReadResult{}, errors.New("storage evidence destination is nil") }
	var total uint64; var hash string
	if err := s.db.QueryRowContext(ctx, "SELECT actual_size, payload_sha256 FROM capture_events WHERE id = ?", eventID).Scan(&total, &hash); errors.Is(err, sql.ErrNoRows) { return ReadResult{}, ErrEventNotFound } else if err != nil { return ReadResult{}, err }
	result := ReadResult{TotalSize:total, SHA256:hash}
	if offset >= total { return result, nil }
	remaining := total-offset; if limit != 0 && limit < remaining { remaining = limit }
	first, last := offset/ChunkSize, (offset+remaining-1)/ChunkSize
	maxSQLiteInt := uint64(^uint64(0) >> 1)
	if first > maxSQLiteInt || last > maxSQLiteInt { return ReadResult{}, ErrEventConflict }
	rows, err := s.db.QueryContext(ctx, "SELECT chunk_index, content FROM capture_event_chunks WHERE event_id = ? AND chunk_index >= ? AND chunk_index <= ? ORDER BY chunk_index", eventID, first, last); if err != nil { return ReadResult{}, err }; defer rows.Close()
	for expected := first; rows.Next(); expected++ {
		if err := ctx.Err(); err != nil { return ReadResult{}, err }
		var index int; var chunk []byte
		if err := rows.Scan(&index, &chunk); err != nil { return ReadResult{}, err }
		if index < 0 || uint64(index) != expected || !validChunk(chunk, expected, total) { return ReadResult{}, ErrEventConflict }
		chunkStart := expected * ChunkSize
		start := uint64(0); if offset > chunkStart { start = offset-chunkStart }
		available := uint64(len(chunk))-start; if available > remaining { available=remaining }
		if err := writeAll(destination, chunk[start:start+available]); err != nil { return ReadResult{}, err }
		result.BytesWritten += available; remaining -= available
		if remaining == 0 { break }
	}
	if err := rows.Err(); err != nil { return ReadResult{}, err }
	if remaining != 0 { return ReadResult{}, ErrEventConflict }
	return result, nil
}

// SearchEvents searches the rebuildable FTS projection. Each valid UTF-8 chunk is indexed separately, so terms spanning chunk boundaries do not match.
func (s *Store) SearchEvents(ctx context.Context, query string) ([]EventReference, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT e.id, e.project_id, e.session_id, COALESCE(e.exchange_id, ''), e.sequence FROM capture_fts f JOIN capture_events e ON e.id = f.event_id WHERE capture_fts MATCH ? ORDER BY e.project_id, e.session_id, e.sequence, e.id`, query)
	if err != nil { return nil, err }; defer rows.Close()
	var matches []EventReference
	for rows.Next() { var match EventReference; if err:=rows.Scan(&match.ID,&match.ProjectID,&match.SessionID,&match.ExchangeID,&match.Sequence); err != nil{return nil,err}; matches=append(matches,match) }
	return matches, rows.Err()
}

// EventCount returns the number of immutable rows using eventID.
func (s *Store) EventCount(ctx context.Context, eventID string) (int, error) { var count int; err:=s.db.QueryRowContext(ctx,"SELECT count(*) FROM capture_events WHERE id = ?",eventID).Scan(&count); return count,err }

func configure(ctx context.Context, db *sql.DB) (Settings, error) {
	if _,err:=db.ExecContext(ctx,"PRAGMA journal_mode=WAL"); err!=nil{return Settings{},err}; if _,err:=db.ExecContext(ctx,"PRAGMA foreign_keys=ON");err!=nil{return Settings{},err}; if _,err:=db.ExecContext(ctx,"PRAGMA busy_timeout=5000");err!=nil{return Settings{},err}; if _,err:=db.ExecContext(ctx,"PRAGMA synchronous=NORMAL");err!=nil{return Settings{},err}
	var journal string; var foreign int; var busy int
	if err:=db.QueryRowContext(ctx,"PRAGMA journal_mode").Scan(&journal);err!=nil{return Settings{},err}; if err:=db.QueryRowContext(ctx,"PRAGMA foreign_keys").Scan(&foreign);err!=nil{return Settings{},err}; if err:=db.QueryRowContext(ctx,"PRAGMA busy_timeout").Scan(&busy);err!=nil{return Settings{},err}
	if strings.ToLower(journal)!="wal" || foreign!=1 || busy<=0{return Settings{},fmt.Errorf("SQLite settings not effective: journal=%q foreign_keys=%d busy_timeout=%d",journal,foreign,busy)}
	return Settings{JournalMode:strings.ToLower(journal),ForeignKeys:foreign==1,BusyTimeout:time.Duration(busy)*time.Millisecond},nil
}
func migrate(ctx context.Context, db *sql.DB, files embed.FS) error { entries,err:=fs.ReadDir(files,".");if err!=nil{return err};sort.Slice(entries,func(i,j int)bool{return entries[i].Name()<entries[j].Name()});for _,entry:=range entries {if entry.IsDir()||!strings.HasSuffix(entry.Name(),".sql"){continue}; versionText:=strings.SplitN(entry.Name(),"_",2)[0];version,err:=strconv.Atoi(versionText);if err!=nil{return fmt.Errorf("migration %q version: %w",entry.Name(),err)};var applied int; if err:=db.QueryRowContext(ctx,"SELECT count(*) FROM schema_migrations WHERE version=?",version).Scan(&applied);err!=nil && !strings.Contains(err.Error(),"no such table"){return err};if applied>0{continue};contents,err:=files.ReadFile(entry.Name());if err!=nil{return err};tx,err:=db.BeginTx(ctx,nil);if err!=nil{return err};if _,err=tx.ExecContext(ctx,string(contents));err==nil {_,err=tx.ExecContext(ctx,"INSERT INTO schema_migrations(version, applied_at) VALUES (?, ?)",version,time.Now().UTC().Format(time.RFC3339Nano))};if err!=nil{_ = tx.Rollback();return fmt.Errorf("apply migration %s: %w",entry.Name(),err)};if err=tx.Commit();err!=nil{return err}};return nil }
func consumeEvidence(ctx context.Context, source io.Reader, declared uint64, consume func([]byte) error) (uint64,string,error) { hash:=sha256.New();buffer:=make([]byte,ChunkSize);remaining:=declared;for remaining>0 {if err:=ctx.Err();err!=nil{return 0,"",err};n,err:=source.Read(buffer[:minUint64(uint64(len(buffer)),remaining)]);if n>0 {chunk:=buffer[:n];if _,writeErr:=hash.Write(chunk);writeErr!=nil{return 0,"",writeErr};if consume!=nil {if consumeErr:=consume(chunk);consumeErr!=nil{return 0,"",consumeErr}};remaining-=uint64(n)};if err!=nil {if err==io.EOF&&remaining>0{return 0,"",io.ErrUnexpectedEOF};if remaining>0{return 0,"",err}};if n==0{return 0,"",io.ErrNoProgress}};return declared,hex.EncodeToString(hash.Sum(nil)),nil }
func storedEvidence(ctx context.Context, tx *sql.Tx, eventID string, declared uint64) (uint64, string, error) { rows,err:=tx.QueryContext(ctx,"SELECT chunk_index, content FROM capture_event_chunks WHERE event_id = ? ORDER BY chunk_index",eventID);if err!=nil{return 0,"",err};defer rows.Close();hash:=sha256.New();var size uint64;expected:=0;for rows.Next(){if err:=ctx.Err();err!=nil{return 0,"",err};var index int;var chunk []byte;if err:=rows.Scan(&index,&chunk);err!=nil{return 0,"",err};if index!=expected||!validChunk(chunk,uint64(index),declared){return 0,"",ErrEventConflict};if _,err:=hash.Write(chunk);err!=nil{return 0,"",err};size+=uint64(len(chunk));expected++};if err:=rows.Err();err!=nil{return 0,"",err};if size!=declared{return 0,"",ErrEventConflict};return size,hex.EncodeToString(hash.Sum(nil)),nil }
func validChunk(chunk []byte, index,total uint64) bool { if total==0||index>(total-1)/ChunkSize{return false};start:=index*ChunkSize;want:=total-start;if want>ChunkSize{want=ChunkSize};return uint64(len(chunk))==want }
func minUint64(a,b uint64) int {if a>b{return int(b)};return int(a)}
func nullable(value string) any {if value=="" {return nil};return value}
func writeAll(w io.Writer, data []byte) error {for len(data)>0 {n,err:=w.Write(data);if n<0||n>len(data){return errors.New("invalid write count")};data=data[n:];if err!=nil{return err};if n==0{return io.ErrShortWrite}};return nil}
func protectDatabase(path string) error {if err:=os.Chmod(path,0o600);err!=nil{return fmt.Errorf("protect database file: %w",err)};return nil}
