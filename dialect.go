package grc

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"github.com/pressly/goose/v3/database"
	"time"
)

func NewStore(tableName, tableEngine, clusterName string) database.Store {
	return &Store{
		tableName:   tableName,
		tableEngine: tableEngine,
		clusterName: clusterName,
	}
}

type Store struct {
	tableName   string
	tableEngine string
	clusterName string
}

func (s *Store) Tablename() string {
	if s.tableName != "" {
		return s.tableName
	}

	return "goose_db_version"
}

func (s *Store) TableEngine() string {
	if s.tableEngine != "" {
		return s.tableEngine
	}

	return fmt.Sprintf("ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/%s', '{replica}')", s.Tablename())
}

func (s *Store) CreateVersionTable(ctx context.Context, db database.DBTxConn) error {
	_, err := db.ExecContext(ctx, fmt.Sprintf(`
		CREATE TABLE IF NOT EXISTS %s ON CLUSTER %s (
			version_id Int64,
			timestamp DateTime DEFAULT now(),
			PRIMARY KEY (version_id)
		) ENGINE = %s;
	`, s.Tablename(), s.clusterName, s.TableEngine()))

	return err
}

func (s *Store) Insert(ctx context.Context, db database.DBTxConn, req database.InsertRequest) error {
	_, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`INSERT INTO %s (version_id) SETTINGS insert_quorum='auto', insert_quorum_parallel=0, select_sequential_consistency=1 VALUES (?)`, s.Tablename()),
		req.Version,
	)

	return err
}

func (s *Store) Delete(ctx context.Context, db database.DBTxConn, version int64) error {
	_, err := db.ExecContext(
		ctx,
		fmt.Sprintf(`ALTER TABLE %s ON CLUSTER %s DELETE WHERE version_id = ? SETTINGS mutations_sync = 2`, s.Tablename(), s.clusterName),
		version,
	)

	return err
}

func (s *Store) GetMigration(ctx context.Context, db database.DBTxConn, version int64) (*database.GetMigrationResult, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT * FROM %s FINAL WHERE version_id = ?`, s.Tablename()), version)
	if row.Err() != nil {
		if errors.Is(row.Err(), sql.ErrNoRows) {
			return nil, database.ErrVersionNotFound
		}

		return nil, row.Err()
	}

	var versionID int64
	var timestamp time.Time
	if err := row.Scan(&versionID, &timestamp); err != nil {
		return nil, err
	}

	return &database.GetMigrationResult{
		Timestamp: timestamp,
		IsApplied: true,
	}, nil
}

func (s *Store) GetLatestVersion(ctx context.Context, db database.DBTxConn) (int64, error) {
	row := db.QueryRowContext(ctx, fmt.Sprintf(`SELECT COALESCE(MAX(version_id), 0) FROM %s FINAL`, s.Tablename()))
	if row.Err() != nil {
		return 0, row.Err()
	}

	var version int64
	if err := row.Scan(&version); err != nil {
		return 0, err
	}

	return version, nil
}

func (s *Store) ListMigrations(ctx context.Context, db database.DBTxConn) ([]*database.ListMigrationsResult, error) {
	rows, err := db.QueryContext(ctx, fmt.Sprintf(`SELECT * FROM %s FINAL ORDER BY version_id`, s.Tablename()))
	if err != nil {
		return nil, err
	}
	defer func() {
		_ = rows.Close()
	}()

	var versionID int64
	var timestamp time.Time
	results := make([]*database.ListMigrationsResult, 0)
	for rows.Next() {
		if err := rows.Scan(&versionID, &timestamp); err != nil {
			return nil, err
		}

		results = append(results, &database.ListMigrationsResult{
			Version:   versionID,
			IsApplied: true,
		})
	}
	if err := rows.Err(); err != nil {
		return nil, err
	}

	return results, nil
}
