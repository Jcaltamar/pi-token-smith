CREATE TABLE schema_migrations (
    version INTEGER PRIMARY KEY,
    applied_at TEXT NOT NULL
);

CREATE TABLE projects (
    id TEXT PRIMARY KEY,
    created_at TEXT NOT NULL
);

CREATE TABLE sessions (
    id TEXT PRIMARY KEY,
    project_id TEXT NOT NULL REFERENCES projects(id),
    created_at TEXT NOT NULL
);
CREATE INDEX sessions_project_id_idx ON sessions(project_id);

CREATE TABLE capture_events (
    id TEXT PRIMARY KEY,
    protocol_version INTEGER NOT NULL,
    event_type TEXT NOT NULL,
    project_id TEXT NOT NULL REFERENCES projects(id),
    session_id TEXT NOT NULL REFERENCES sessions(id),
    exchange_id TEXT,
    sequence INTEGER NOT NULL,
    occurred_at TEXT NOT NULL,
    received_at TEXT NOT NULL,
    payload_encoding TEXT NOT NULL,
    declared_size INTEGER NOT NULL,
    actual_size INTEGER NOT NULL,
    payload_sha256 TEXT NOT NULL,
    CHECK (declared_size >= 0),
    CHECK (actual_size >= 0)
);
CREATE INDEX capture_events_project_session_sequence_idx ON capture_events(project_id, session_id, sequence);
CREATE INDEX capture_events_exchange_id_idx ON capture_events(exchange_id);

CREATE TRIGGER capture_events_no_update
BEFORE UPDATE ON capture_events
WHEN OLD.payload_sha256 <> ''
BEGIN
    SELECT RAISE(ABORT, 'capture_events are immutable');
END;

CREATE TRIGGER capture_events_no_delete
BEFORE DELETE ON capture_events
BEGIN
    SELECT RAISE(ABORT, 'capture_events are immutable');
END;

CREATE TABLE capture_event_chunks (
    event_id TEXT NOT NULL REFERENCES capture_events(id) ON DELETE CASCADE,
    chunk_index INTEGER NOT NULL,
    content BLOB NOT NULL,
    PRIMARY KEY (event_id, chunk_index)
);

CREATE TRIGGER capture_event_chunks_no_update
BEFORE UPDATE ON capture_event_chunks
BEGIN
    SELECT RAISE(ABORT, 'capture_event_chunks are immutable');
END;

CREATE TRIGGER capture_event_chunks_no_delete
BEFORE DELETE ON capture_event_chunks
BEGIN
    SELECT RAISE(ABORT, 'capture_event_chunks are immutable');
END;

CREATE VIRTUAL TABLE capture_fts USING fts5(
    event_id UNINDEXED,
    project_id UNINDEXED,
    session_id UNINDEXED,
    exchange_id UNINDEXED,
    content
);
