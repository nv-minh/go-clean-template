// Command migrate applies the embedded SQL migrations.
//
//	migrate up            apply all pending migrations
//	migrate down [N]      roll back N migrations (default 1)
//	migrate version       print the current version
//	migrate force V       set the version without running anything (fix a dirty state)
//
// The database URL comes from DB_URL. Run it as a Kubernetes Job / init step BEFORE rolling out
// new application versions, and write migrations backwards compatible (expand, then contract).
package main

import (
	"errors"
	"fmt"
	"os"
	"strconv"
	"strings"

	"github.com/golang-migrate/migrate/v4"
	_ "github.com/golang-migrate/migrate/v4/database/pgx/v5"
	"github.com/golang-migrate/migrate/v4/source/iofs"

	"github.com/yourorg/go-clean-template/db"
)

func main() {
	if err := run(os.Args[1:]); err != nil {
		fmt.Fprintln(os.Stderr, "migrate:", err)
		os.Exit(1)
	}
}

func run(args []string) error {
	if len(args) == 0 {
		return errors.New("usage: migrate up | down [N] | version | force V")
	}
	url := os.Getenv("DB_URL")
	if url == "" {
		return errors.New("DB_URL is required")
	}
	// golang-migrate selects the driver from the URL scheme.
	url = strings.Replace(strings.Replace(url, "postgresql://", "pgx5://", 1), "postgres://", "pgx5://", 1)

	src, err := iofs.New(db.Migrations, "migrations")
	if err != nil {
		return fmt.Errorf("open embedded migrations: %w", err)
	}
	m, err := migrate.NewWithSourceInstance("iofs", src, url)
	if err != nil {
		return fmt.Errorf("init migrate: %w", err)
	}
	defer func() { _, _ = m.Close() }()

	switch args[0] {
	case "up":
		err = m.Up()
	case "down":
		n := 1
		if len(args) > 1 {
			if n, err = strconv.Atoi(args[1]); err != nil || n < 1 {
				return errors.New("down expects a positive number of steps")
			}
		}
		err = m.Steps(-n)
	case "version":
		v, dirty, verr := m.Version()
		if verr != nil {
			return verr
		}
		fmt.Printf("version=%d dirty=%t\n", v, dirty)
		return nil
	case "force":
		if len(args) < 2 {
			return errors.New("force expects a version")
		}
		v, perr := strconv.Atoi(args[1])
		if perr != nil {
			return perr
		}
		err = m.Force(v)
	default:
		return fmt.Errorf("unknown command %q", args[0])
	}
	if errors.Is(err, migrate.ErrNoChange) {
		fmt.Println("no change")
		return nil
	}
	if err == nil {
		fmt.Println("done")
	}
	return err
}
