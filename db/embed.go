// Package db embeds the SQL migrations so the migrate binary is fully self contained.
package db

import "embed"

//go:embed migrations/*.sql
var Migrations embed.FS
