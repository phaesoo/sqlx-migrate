package migrate

import (
	"fmt"
	"testing"

	"github.com/jmoiron/sqlx"
	_ "github.com/mattn/go-sqlite3"
	"github.com/stretchr/testify/assert"
)

var migrateFunc1 = func(tx *sqlx.Tx) error {
	if _, err := tx.Exec(`
	CREATE TABLE courses (
	  id serial PRIMARY KEY,
	  name text
	);`); err != nil {
		return err
	}
	return nil
}
var rollbackFunc1 = func(tx *sqlx.Tx) error {
	if _, err := tx.Exec(`DROP TABLE courses;`); err != nil {
		return err
	}
	return nil
}
var migrateFunc2 = func(tx *sqlx.Tx) error {
	if _, err := tx.Exec(`
	CREATE TABLE users (
	  id serial PRIMARY KEY,
	  email text UNIQUE NOT NULL
	);`); err != nil {
		return err
	}
	return nil
}
var rollbackFunc2 = func(tx *sqlx.Tx) error {
	if _, err := tx.Exec(`DROP TABLE users;`); err != nil {
		return err
	}
	return nil
}

func sqliteInMem(t *testing.T) *sqlx.DB {
	db, err := sqlx.Open("sqlite3", fmt.Sprintf("file:%s?mode=memory&cache=shared", t.Name()))
	if err != nil {
		t.Fatalf("Open() err = %v; want nil", err)
	}
	t.Cleanup(func() {
		err = db.Close()
		if err != nil {
			t.Errorf("Close() err = %v; want nil", err)
		}
	})
	return db
}

func TestSqlx(t *testing.T) {
	t.Run("simple", func(t *testing.T) {
		db := sqliteInMem(t)

		migration := Migration{
			ID:       "1",
			Name:     "001_create_courses",
			Migrate:  migrateFunc1,
			Rollback: nil,
		}

		migrator := New(db, []Migration{migration})

		if err := migrator.Migrate(); err != nil {
			t.Fatalf("Migrate() err = %v; want nil", err)
		}
		if _, err := db.Exec("INSERT INTO courses (name) VALUES (?) ", "cor_test"); err != nil {
		}
	})

	t.Run("existing migrations", func(t *testing.T) {
		db := sqliteInMem(t)

		migration := Migration{
			ID:       "1",
			Name:     "001_create_courses",
			Migrate:  migrateFunc1,
			Rollback: nil,
		}
		migrator := New(db, []Migration{migration})

		err := migrator.Migrate()
		assert.NoError(t, err, "Migrate() err = %v; want nil")

		_, err = db.Exec("INSERT INTO courses (name) VALUES (?) ", "cor_test")
		assert.NoError(t, err, "db.Exec() err = %v; want nil")

		// the real test
		newMigrator := New(db, []Migration{
			{
				ID:       "1",
				Name:     "001_create_courses",
				Migrate:  migrateFunc1,
				Rollback: nil,
			},
			{
				ID:       "2",
				Name:     "002_create_users",
				Migrate:  migrateFunc2,
				Rollback: nil,
			},
		})

		err = newMigrator.Migrate()
		assert.NoError(t, err, "Migrate() err = %v; want nil")
		_, err = db.Exec("INSERT INTO users (email) VALUES (?) ", "abc@test.com")
		assert.NoError(t, err, "db.Exec() err = %v; want nil")
	})

	t.Run("rollback", func(t *testing.T) {
		db := sqliteInMem(t)

		migration := Migration{
			ID:       "1",
			Name:     "001_create_courses",
			Migrate:  migrateFunc1,
			Rollback: rollbackFunc1,
		}
		migrator := New(db, []Migration{migration})

		err := migrator.Migrate()
		assert.NoError(t, err, "Migrate() err = %v; want nil")

		_, err = db.Exec("INSERT INTO courses (name) VALUES (?) ", "cor_test")
		assert.NoError(t, err, "db.Exec() err = %v; want nil")

		err = migrator.Rollback()
		assert.NoError(t, err, "Rollback() err = %v; want nil")

		var count int
		err = db.QueryRow("SELECT COUNT(id) FROM courses;").Scan(&count)
		assert.Error(t, err, "db.QueryRow() err = nil; want table missing error")

		// Don't want to test inner workings of lib, so let's just migrate again and verify we have a table now
		err = migrator.Migrate()
		assert.NoError(t, err, "Migrate() err = %v; want nil", err)

		_, err = db.Exec("INSERT INTO courses (name) VALUES (?) ", "cor_test")
		assert.NoError(t, err, "db.Exec() err = %v; want nil", err)

		err = db.QueryRow("SELECT COUNT(*) FROM courses;").Scan(&count)
		assert.NoError(t, err, "db.QueryRow() err = %v; want nil", err)
		assert.Equalf(t, 1, count, "count = %d; want %d", count, 1)
	})
}
