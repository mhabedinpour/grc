# GRC - Goose ReplicatedClickhouse

GRC is a custom implementation of the [Goose](https://github.com/pressly/goose) database migration tool's store interface for ClickHouse databases, specifically designed to work with ReplicatedReplacingMergeTree engine.

## Overview

This package provides a ClickHouse dialect for Goose migrations, allowing you to manage database schema migrations in ClickHouse clusters with proper replication support.

## Features

- Support for ClickHouse's ReplicatedReplacingMergeTree engine
- Cluster-aware migrations
- Custom table name and engine configuration
- Full compatibility with Goose migration workflows

## Installation

```bash
go get github.com/mhabedinpour/grc
```

## Usage

```go
// Connect to ClickHouse
db, err := sql.Open("clickhouse", "clickhouse://user:password@localhost:9000/database")
if err != nil {
    return err
}
defer db.Close()

// Create a new GRC store
store := grc.NewStore(
    "goose_migrations",     // Custom table name (optional, defaults to `goose_db_version`)
    "",                     // Custom table engine (optional, defaults to `ReplicatedReplacingMergeTree('/clickhouse/tables/{shard}/goose_db_version', '{replica}')`)
    "my_cluster"            // ClickHouse cluster name
)

// Setup provider
provider, err := goose.NewProvider(
    "",
    db,
    fs,
    goose.WithStore(store),
)
if err != nil {
    return err
}
defer provider.Close()

// Migrate
if _, err := provider.Up(ctx); err != nil {
    return err
}
```