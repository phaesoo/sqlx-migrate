package migrate

import (
	"database/sql"
	"fmt"
	"log"

	"github.com/jmoiron/sqlx"
)

var supportedDrivers = map[string]bool{
	"sqlite3": true,
	"mysql":   true,
}

// SqlxMigrate is a migrator that uses github.com/jmoiron/sqlx
type SqlxMigrate struct {
	db         *sqlx.DB
	migrations []Migration
}

// New creates a SqlxMigrate instance
func New(db *sqlx.DB, migrations []Migration) (*SqlxMigrate, error) {
	if _, ok := supportedDrivers[db.DriverName()]; !ok {
		return nil, fmt.Errorf("Unsupported driver name: %s", db.DriverName())
	}

	return &SqlxMigrate{
		db:         db,
		migrations: migrations,
	}, nil
}

// Migrate will run the migrations using the provided db connection.
func (m *SqlxMigrate) Migrate() error {
	err := m.createMigrationTable()
	if err != nil {
		return err
	}

	for _, migration := range m.migrations {
		var found string
		err := m.db.Get(&found, "SELECT id FROM migrations WHERE id=?", migration.ID)
		switch err {
		case sql.ErrNoRows:
			log.Printf("Running migration: %v\n", migration.ID)
			// we need to run the migration so we continue to code below
		case nil:
			log.Printf("Skipping migration: %v\n", migration.ID)
			continue
		default:
			return fmt.Errorf("looking up migration by id: %w", err)
		}
		err = m.runMigration(migration)
		if err != nil {
			return err
		}
	}
	return nil
}

// Rollback will run all rollbacks using the provided db connection.
func (m *SqlxMigrate) Rollback() error {
	err := m.createMigrationTable()
	if err != nil {
		return err
	}
	for i := len(m.migrations) - 1; i >= 0; i-- {
		migration := m.migrations[i]
		if migration.Rollback == nil {
			log.Printf("Rollback not provided: %v\n", migration.ID)
			continue
		}
		var found string
		err := m.db.Get(&found, "SELECT id FROM migrations WHERE id=?", migration.ID)
		switch err {
		case sql.ErrNoRows:
			log.Printf("Skipping rollback: %v\n", migration.ID)
			continue
		case nil:
			log.Printf("Running rollback: %v\n", migration.ID)
			// we need to run the rollback so we continue to code below
		default:
			return fmt.Errorf("looking up rollback by id: %w", err)
		}
		err = m.runRollback(migration)
		if err != nil {
			return err
		}
	}
	return nil
}

func (m *SqlxMigrate) createMigrationTable() error {
	_, err := m.db.Exec("CREATE TABLE IF NOT EXISTS migrations (id VARCHAR(63) PRIMARY KEY )")
	if err != nil {
		return fmt.Errorf("creating migrations table: %w", err)
	}
	return nil
}

func (m *SqlxMigrate) runMigration(migration Migration) error {
	errorf := func(err error) error { return fmt.Errorf("running migration: %w", err) }

	tx, err := m.db.Beginx()
	if err != nil {
		return errorf(err)
	}
	_, err = tx.Exec("INSERT INTO migrations (id) VALUES (?)", migration.ID)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = migration.Migrate(tx)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = tx.Commit()
	if err != nil {
		return errorf(err)
	}
	return nil
}

func (m *SqlxMigrate) runRollback(migration Migration) error {
	errorf := func(err error) error { return fmt.Errorf("running rollback: %w", err) }

	tx, err := m.db.Beginx()
	if err != nil {
		return errorf(err)
	}
	_, err = tx.Exec("DELETE FROM migrations WHERE id=?", migration.ID)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = migration.Rollback(tx)
	if err != nil {
		tx.Rollback()
		return errorf(err)
	}
	err = tx.Commit()
	if err != nil {
		return errorf(err)
	}
	return nil
}

// Migration is a unique ID plus a function that uses a sqlx transaction
// to perform a database migration step.
type Migration struct {
	ID       string
	Name     string
	Migrate  func(tx *sqlx.Tx) error
	Rollback func(tx *sqlx.Tx) error
}
