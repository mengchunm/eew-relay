package main

import (
	"context"
	"database/sql"
	"fmt"
	"strings"
	"sync"
	"time"

	_ "github.com/go-sql-driver/mysql"
)

var barkMySQLPools sync.Map

func selfHostedBarkKeyExistsMySQL(dsn, barkID string) (bool, error) {
	db, err := barkMySQLPool(dsn)
	if err != nil {
		return false, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE `+"`key`"+`=? LIMIT 1)`, barkID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}

func barkMySQLPool(dsn string) (*sql.DB, error) {
	dsn = strings.TrimSpace(dsn)
	value, ok := barkMySQLPools.Load(dsn)
	if !ok {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return nil, err
		}
		db.SetMaxOpenConns(8)
		db.SetMaxIdleConns(2)
		db.SetConnMaxIdleTime(5 * time.Minute)
		actual, loaded := barkMySQLPools.LoadOrStore(dsn, db)
		if loaded {
			_ = db.Close()
			value = actual
		} else {
			value = db
		}
	}
	db, ok := value.(*sql.DB)
	if !ok {
		return nil, fmt.Errorf("invalid Bark MySQL pool")
	}
	return db, nil
}

func selfHostedBarkKeysMySQL(dsn string) (map[string]struct{}, error) {
	db, err := barkMySQLPool(dsn)
	if err != nil {
		return nil, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	rows, err := db.QueryContext(ctx, `SELECT `+"`key`"+` FROM devices`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	keys := make(map[string]struct{})
	for rows.Next() {
		var key string
		if err := rows.Scan(&key); err != nil {
			return nil, err
		}
		key = strings.TrimSpace(key)
		if key != "" {
			keys[key] = struct{}{}
		}
	}
	return keys, rows.Err()
}
