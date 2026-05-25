package configrbac

import (
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/opencost/opencost/core/pkg/env"
	"github.com/opencost/opencost/core/pkg/util/json"
	_ "modernc.org/sqlite"
)

const scopedViewsDBFile = "rbac/scoped_views.db"

// Store persists RBAC data in SQLite. The database file is created lazily on first use.
type Store struct {
	dbPath string
	mu     sync.Mutex
}

// NewStore opens the default scoped views database path under CONFIG_PATH.
func NewStore() *Store {
	return &Store{dbPath: env.GetPathFromConfig(scopedViewsDBFile)}
}

// NewStoreAt is used by tests to override the database path.
func NewStoreAt(dbPath string) *Store {
	return &Store{dbPath: dbPath}
}

// DBPath returns the SQLite file path.
func (s *Store) DBPath() string {
	return s.dbPath
}

func (s *Store) open() (*sql.DB, error) {
	if err := os.MkdirAll(filepath.Dir(s.dbPath), 0o755); err != nil {
		return nil, fmt.Errorf("create rbac db directory: %w", err)
	}

	// Create an empty database file if it does not exist yet.
	if _, err := os.Stat(s.dbPath); errors.Is(err, os.ErrNotExist) {
		f, err := os.OpenFile(s.dbPath, os.O_RDWR|os.O_CREATE, 0o644)
		if err != nil {
			return nil, fmt.Errorf("create rbac db file: %w", err)
		}
		_ = f.Close()
	} else if err != nil {
		return nil, fmt.Errorf("stat rbac db file: %w", err)
	}

	db, err := sql.Open("sqlite", s.dbPath)
	if err != nil {
		return nil, fmt.Errorf("open rbac db: %w", err)
	}
	db.SetMaxOpenConns(1)

	if err := migrate(db); err != nil {
		_ = db.Close()
		return nil, err
	}
	return db, nil
}

func migrate(db *sql.DB) error {
	stmts := []string{
		`CREATE TABLE IF NOT EXISTS scoped_views (
			id TEXT PRIMARY KEY NOT NULL,
			name TEXT NOT NULL,
			payload TEXT NOT NULL,
			created_at TEXT NOT NULL,
			updated_at TEXT NOT NULL
		);`,
	}
	for _, stmt := range stmts {
		if _, err := db.Exec(stmt); err != nil {
			return fmt.Errorf("migrate rbac schema: %w", err)
		}
	}
	return nil
}

func (s *Store) withDB(fn func(*sql.DB) error) error {
	s.mu.Lock()
	defer s.mu.Unlock()

	db, err := s.open()
	if err != nil {
		return err
	}
	defer db.Close()

	return fn(db)
}

// List returns all scoped views ordered by name.
func (s *Store) List() ([]ScopedView, error) {
	var views []ScopedView
	err := s.withDB(func(db *sql.DB) error {
		rows, err := db.Query(`
			SELECT payload FROM scoped_views ORDER BY name COLLATE NOCASE ASC;
		`)
		if err != nil {
			return err
		}
		defer rows.Close()

		for rows.Next() {
			var payload string
			if err := rows.Scan(&payload); err != nil {
				return err
			}
			var view ScopedView
			if err := json.Unmarshal([]byte(payload), &view); err != nil {
				return fmt.Errorf("decode scoped view: %w", err)
			}
			views = append(views, view)
		}
		return rows.Err()
	})
	if views == nil {
		views = []ScopedView{}
	}
	return views, err
}

// Get returns a scoped view by id.
func (s *Store) Get(id string) (ScopedView, error) {
	var view ScopedView
	err := s.withDB(func(db *sql.DB) error {
		var payload string
		err := db.QueryRow(`SELECT payload FROM scoped_views WHERE id = ?;`, id).Scan(&payload)
		if errors.Is(err, sql.ErrNoRows) {
			return sql.ErrNoRows
		}
		if err != nil {
			return err
		}
		return json.Unmarshal([]byte(payload), &view)
	})
	return view, err
}

// Create inserts a new scoped view.
func (s *Store) Create(view ScopedView) error {
	return s.withDB(func(db *sql.DB) error {
		payload, err := json.Marshal(view)
		if err != nil {
			return err
		}
		_, err = db.Exec(`
			INSERT INTO scoped_views (id, name, payload, created_at, updated_at)
			VALUES (?, ?, ?, ?, ?);
		`, view.ID, view.Name, string(payload), view.CreatedAt, view.UpdatedAt)
		if err != nil {
			return err
		}
		return nil
	})
}

// Update replaces an existing scoped view.
func (s *Store) Update(view ScopedView) error {
	return s.withDB(func(db *sql.DB) error {
		payload, err := json.Marshal(view)
		if err != nil {
			return err
		}
		res, err := db.Exec(`
			UPDATE scoped_views
			SET name = ?, payload = ?, updated_at = ?
			WHERE id = ?;
		`, view.Name, string(payload), view.UpdatedAt, view.ID)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

// Delete removes a scoped view by id.
func (s *Store) Delete(id string) error {
	return s.withDB(func(db *sql.DB) error {
		res, err := db.Exec(`DELETE FROM scoped_views WHERE id = ?;`, id)
		if err != nil {
			return err
		}
		n, err := res.RowsAffected()
		if err != nil {
			return err
		}
		if n == 0 {
			return sql.ErrNoRows
		}
		return nil
	})
}

// Exists reports whether a scoped view id is already stored.
func (s *Store) Exists(id string) (bool, error) {
	var found bool
	err := s.withDB(func(db *sql.DB) error {
		err := db.QueryRow(`SELECT 1 FROM scoped_views WHERE id = ? LIMIT 1;`, id).Scan(new(int))
		if errors.Is(err, sql.ErrNoRows) {
			return nil
		}
		if err != nil {
			return err
		}
		found = true
		return nil
	})
	return found, err
}

func nowRFC3339() string {
	return time.Now().UTC().Format(time.RFC3339)
}
