CREATE TABLE capture_attempts (
    attempt_id TEXT PRIMARY KEY,
    event_id TEXT NOT NULL,
    event_type TEXT NOT NULL,
    protocol_version INTEGER NOT NULL,
    project_id TEXT NOT NULL,
    session_id TEXT NOT NULL,
    exchange_id TEXT,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    payload_encoding TEXT NOT NULL,
    declared_size INTEGER NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('pending', 'completed', 'capture_incomplete')),
    started_at TEXT NOT NULL,
    completed_at TEXT,
    failure_stage TEXT,
    CHECK (declared_size >= 0)
);
CREATE INDEX capture_attempts_status_idx ON capture_attempts(status);
CREATE INDEX capture_attempts_event_id_idx ON capture_attempts(event_id);
CREATE INDEX capture_attempts_session_id_idx ON capture_attempts(session_id);
