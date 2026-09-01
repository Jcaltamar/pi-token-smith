// Package migrations embeds the SQLite schema migrations shipped with the daemon.
package migrations

import "embed"

// Files contains ordered SQL migrations.
//
//go:embed *.sql
var Files embed.FS
