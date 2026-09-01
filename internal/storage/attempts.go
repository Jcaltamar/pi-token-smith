package storage

import (
	"context"
	"database/sql"
	"encoding/hex"
	"errors"
	"fmt"
	"time"
)

// CaptureAttemptStatus records the terminal state of a capture attempt.
type CaptureAttemptStatus string

const (
	CaptureAttemptPending    CaptureAttemptStatus = "pending"
	CaptureAttemptCompleted  CaptureAttemptStatus = "completed"
	CaptureAttemptIncomplete CaptureAttemptStatus = "capture_incomplete"
)

// CaptureFailureStage identifies the safe boundary at which a capture failed.
type CaptureFailureStage string

const (
	CaptureFailureHeader CaptureFailureStage = "header"
	CaptureFailureAppend CaptureFailureStage = "append"
)

// Attempt identifies the immutable metadata protected by one capture attempt.
type Attempt struct {
	AttemptID, EventID, EventType, ProjectID, SessionID, ExchangeID, Encoding string
	ProtocolVersion                                                           int
	Sequence                                                                  int64
	OccurredAt                                                                time.Time
	DeclaredSize                                                              uint64
	Status                                                                    CaptureAttemptStatus
	StartedAt, CompletedAt                                                    time.Time
	FailureStage                                                              CaptureFailureStage
}

// RecoveryResult counts pending attempts reconciled by RecoverPendingCaptures.
type RecoveryResult struct{ Completed, Incomplete int }

// BeginCaptureAttempt records a pending attempt before evidence framing begins.
func (s *Store) BeginCaptureAttempt(ctx context.Context, attempt Attempt) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	var ok bool
	if attempt.OccurredAt, ok = canonicalOccurredAt(attempt.OccurredAt); !ok || !validAttempt(attempt) {
		return ErrEventConflict
	}
	if attempt.DeclaredSize > uint64(^uint64(0)>>1) {
		return ErrEventConflict
	}
	if attempt.Status != "" && attempt.Status != CaptureAttemptPending {
		return ErrEventConflict
	}
	now := time.Now().UTC()
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	_, err = tx.ExecContext(ctx, `INSERT INTO capture_attempts (attempt_id, event_id, event_type, protocol_version, project_id, session_id, exchange_id, sequence, occurred_at, payload_encoding, declared_size, status, started_at) VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)`, attempt.AttemptID, attempt.EventID, attempt.EventType, attempt.ProtocolVersion, attempt.ProjectID, attempt.SessionID, nullable(attempt.ExchangeID), attempt.Sequence, attempt.OccurredAt.Format(time.RFC3339Nano), attempt.Encoding, attempt.DeclaredSize, CaptureAttemptPending, now.Format(time.RFC3339Nano))
	if err == nil {
		return tx.Commit()
	}
	if !isConstraint(err) {
		return err
	}
	existing, getErr := captureAttemptTx(ctx, tx, attempt.AttemptID)
	if getErr != nil {
		return getErr
	}
	if sameAttempt(existing, attempt) && existing.Status == CaptureAttemptPending {
		return tx.Commit()
	}
	return ErrEventConflict
}

// CompleteCaptureAttempt transitions a pending attempt only after durable evidence passes integrity checks.
func (s *Store) CompleteCaptureAttempt(ctx context.Context, attemptID string) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	attempt, err := captureAttemptTx(ctx, tx, attemptID)
	if err != nil {
		return err
	}
	if attempt.Status == CaptureAttemptCompleted {
		return tx.Commit()
	}
	if attempt.Status != CaptureAttemptPending {
		return ErrEventConflict
	}
	valid, err := completeEvidence(ctx, tx, attempt)
	if err != nil {
		return err
	}
	if !valid {
		return ErrEventConflict
	}
	if _, err = tx.ExecContext(ctx, "UPDATE capture_attempts SET status = ?, completed_at = ?, failure_stage = NULL WHERE attempt_id = ? AND status = ?", CaptureAttemptCompleted, time.Now().UTC().Format(time.RFC3339Nano), attemptID, CaptureAttemptPending); err != nil {
		return err
	}
	return tx.Commit()
}

// MarkCaptureIncomplete records a terminal, safe failure stage for a pending attempt.
func (s *Store) MarkCaptureIncomplete(ctx context.Context, attemptID string, stage CaptureFailureStage) error {
	if err := ctx.Err(); err != nil {
		return err
	}
	if !validFailureStage(stage) {
		return ErrEventConflict
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return err
	}
	defer tx.Rollback()
	attempt, err := captureAttemptTx(ctx, tx, attemptID)
	if err != nil {
		return err
	}
	if attempt.Status == CaptureAttemptIncomplete && attempt.FailureStage == stage {
		return tx.Commit()
	}
	if attempt.Status != CaptureAttemptPending {
		return ErrEventConflict
	}
	if _, err = tx.ExecContext(ctx, "UPDATE capture_attempts SET status = ?, completed_at = ?, failure_stage = ? WHERE attempt_id = ? AND status = ?", CaptureAttemptIncomplete, time.Now().UTC().Format(time.RFC3339Nano), stage, attemptID, CaptureAttemptPending); err != nil {
		return err
	}
	return tx.Commit()
}

// RecoverPendingCaptures atomically reconciles all pending attempts with committed evidence.
func (s *Store) RecoverPendingCaptures(ctx context.Context) (RecoveryResult, error) {
	if err := ctx.Err(); err != nil {
		return RecoveryResult{}, err
	}
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return RecoveryResult{}, err
	}
	defer tx.Rollback()
	rows, err := tx.QueryContext(ctx, "SELECT attempt_id, event_id, event_type, protocol_version, project_id, session_id, COALESCE(exchange_id, ''), sequence, occurred_at, payload_encoding, declared_size, status, started_at, completed_at, COALESCE(failure_stage, '') FROM capture_attempts WHERE status = ? ORDER BY attempt_id", CaptureAttemptPending)
	if err != nil {
		return RecoveryResult{}, err
	}
	var pending []Attempt
	for rows.Next() {
		attempt, err := scanAttempt(rows)
		if err != nil {
			_ = rows.Close()
			return RecoveryResult{}, err
		}
		pending = append(pending, attempt)
	}
	if err := rows.Err(); err != nil {
		_ = rows.Close()
		return RecoveryResult{}, err
	}
	if err := rows.Close(); err != nil {
		return RecoveryResult{}, err
	}
	var result RecoveryResult
	for _, attempt := range pending {
		complete, err := completeEvidence(ctx, tx, attempt)
		if err != nil {
			return RecoveryResult{}, err
		}
		if complete {
			_, err = tx.ExecContext(ctx, "UPDATE capture_attempts SET status = ?, completed_at = ?, failure_stage = NULL WHERE attempt_id = ?", CaptureAttemptCompleted, time.Now().UTC().Format(time.RFC3339Nano), attempt.AttemptID)
			result.Completed++
		} else {
			_, err = tx.ExecContext(ctx, "UPDATE capture_attempts SET status = ?, completed_at = ?, failure_stage = ? WHERE attempt_id = ?", CaptureAttemptIncomplete, time.Now().UTC().Format(time.RFC3339Nano), CaptureFailureAppend, attempt.AttemptID)
			result.Incomplete++
		}
		if err != nil {
			return RecoveryResult{}, err
		}
	}
	if err = tx.Commit(); err != nil {
		return RecoveryResult{}, err
	}
	return result, nil
}

// CaptureAttempt returns metadata for diagnostics without evidence content.
func (s *Store) CaptureAttempt(ctx context.Context, attemptID string) (Attempt, error) {
	if err := ctx.Err(); err != nil {
		return Attempt{}, err
	}
	return captureAttemptQuery(ctx, s.db, attemptID)
}

type attemptScanner interface{ Scan(...any) error }
type attemptQueryer interface {
	QueryRowContext(context.Context, string, ...any) *sql.Row
}

func captureAttemptQuery(ctx context.Context, q attemptQueryer, attemptID string) (Attempt, error) {
	return scanAttempt(q.QueryRowContext(ctx, "SELECT attempt_id, event_id, event_type, protocol_version, project_id, session_id, COALESCE(exchange_id, ''), sequence, occurred_at, payload_encoding, declared_size, status, started_at, completed_at, COALESCE(failure_stage, '') FROM capture_attempts WHERE attempt_id = ?", attemptID))
}
func captureAttemptTx(ctx context.Context, tx *sql.Tx, attemptID string) (Attempt, error) {
	return captureAttemptQuery(ctx, tx, attemptID)
}
func scanAttempt(row attemptScanner) (Attempt, error) {
	var attempt Attempt
	var occurred, started string
	var completed sql.NullString
	var stage string
	if err := row.Scan(&attempt.AttemptID, &attempt.EventID, &attempt.EventType, &attempt.ProtocolVersion, &attempt.ProjectID, &attempt.SessionID, &attempt.ExchangeID, &attempt.Sequence, &occurred, &attempt.Encoding, &attempt.DeclaredSize, &attempt.Status, &started, &completed, &stage); errors.Is(err, sql.ErrNoRows) {
		return Attempt{}, ErrEventNotFound
	} else if err != nil {
		return Attempt{}, err
	}
	var err error
	attempt.OccurredAt, err = time.Parse(time.RFC3339Nano, occurred)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse attempt occurrence time: %w", err)
	}
	attempt.OccurredAt = attempt.OccurredAt.UTC()
	attempt.StartedAt, err = time.Parse(time.RFC3339Nano, started)
	if err != nil {
		return Attempt{}, fmt.Errorf("parse attempt start time: %w", err)
	}
	if completed.Valid {
		attempt.CompletedAt, err = time.Parse(time.RFC3339Nano, completed.String)
		if err != nil {
			return Attempt{}, fmt.Errorf("parse attempt completion time: %w", err)
		}
	}
	attempt.FailureStage = CaptureFailureStage(stage)
	return attempt, nil
}

func completeEvidence(ctx context.Context, tx *sql.Tx, attempt Attempt) (bool, error) {
	var eventType, project, session, exchange, occurred, encoding, hash string
	var protocolVersion int
	var sequence int64
	var declared, actual uint64
	err := tx.QueryRowContext(ctx, "SELECT event_type, protocol_version, project_id, session_id, COALESCE(exchange_id, ''), sequence, occurred_at, payload_encoding, declared_size, actual_size, payload_sha256 FROM capture_events WHERE id = ?", attempt.EventID).Scan(&eventType, &protocolVersion, &project, &session, &exchange, &sequence, &occurred, &encoding, &declared, &actual, &hash)
	if errors.Is(err, sql.ErrNoRows) {
		return false, nil
	}
	if err != nil {
		return false, err
	}
	occurredAt, err := time.Parse(time.RFC3339Nano, occurred)
	if err != nil {
		return false, fmt.Errorf("parse event occurrence time: %w", err)
	}
	if eventType != attempt.EventType || protocolVersion != attempt.ProtocolVersion || project != attempt.ProjectID || session != attempt.SessionID || exchange != attempt.ExchangeID || sequence != attempt.Sequence || !occurredAt.UTC().Equal(attempt.OccurredAt) || encoding != attempt.Encoding || declared != attempt.DeclaredSize || actual != declared || !validSHA256(hash) {
		return false, nil
	}
	size, computed, err := storedEvidence(ctx, tx, attempt.EventID, declared)
	if err != nil {
		if errors.Is(err, ErrEventConflict) {
			return false, nil
		}
		return false, err
	}
	return size == actual && computed == hash, nil
}
func validAttempt(a Attempt) bool {
	return a.AttemptID != "" && a.EventID != "" && a.EventType != "" && a.ProtocolVersion > 0 && a.ProjectID != "" && a.SessionID != "" && a.Sequence >= 0 && !a.OccurredAt.IsZero() && a.Encoding != ""
}
func canonicalOccurredAt(value time.Time) (time.Time, bool) {
	if value.IsZero() {
		return time.Time{}, false
	}
	return value.UTC(), true
}
func sameAttempt(a, b Attempt) bool {
	return a.AttemptID == b.AttemptID && a.EventID == b.EventID && a.EventType == b.EventType && a.ProtocolVersion == b.ProtocolVersion && a.ProjectID == b.ProjectID && a.SessionID == b.SessionID && a.ExchangeID == b.ExchangeID && a.Sequence == b.Sequence && a.OccurredAt.Equal(b.OccurredAt) && a.Encoding == b.Encoding && a.DeclaredSize == b.DeclaredSize
}
func validFailureStage(stage CaptureFailureStage) bool {
	return stage == CaptureFailureHeader || stage == CaptureFailureAppend
}
func validSHA256(value string) bool {
	if len(value) != 64 {
		return false
	}
	_, err := hex.DecodeString(value)
	return err == nil
}
func isConstraint(err error) bool {
	return err != nil && (contains(err.Error(), "UNIQUE constraint failed") || contains(err.Error(), "PRIMARY KEY"))
}
func contains(value, part string) bool {
	for i := 0; i+len(part) <= len(value); i++ {
		if value[i:i+len(part)] == part {
			return true
		}
	}
	return false
}
