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
	var usable bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE `+"`key`"+`=? AND LENGTH(TRIM(token)) > 0 LIMIT 1)`, barkID).Scan(&usable); err != nil {
		return false, err
	}
	return usable, nil
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

func selfHostedBarkDevicesMySQL(dsn string) (selfHostedBarkDeviceIndex, error) {
	db, err := barkMySQLPool(dsn)
	if err != nil {
		return selfHostedBarkDeviceIndex{}, err
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	var collation string
	if err := db.QueryRowContext(ctx, `
		SELECT COLLATION_NAME
		FROM information_schema.COLUMNS
		WHERE TABLE_SCHEMA = DATABASE() AND TABLE_NAME = 'devices' AND COLUMN_NAME = 'key'
		LIMIT 1`).Scan(&collation); err != nil {
		return selfHostedBarkDeviceIndex{}, fmt.Errorf("read Bark devices.key collation: %w", err)
	}
	caseInsensitive := strings.HasSuffix(strings.ToLower(strings.TrimSpace(collation)), "_ci")
	rows, err := db.QueryContext(ctx, `SELECT `+"`key`"+`, token FROM devices`)
	if err != nil {
		return selfHostedBarkDeviceIndex{}, err
	}
	defer rows.Close()
	devices := make(map[string]bool)
	for rows.Next() {
		var key, token string
		if err := rows.Scan(&key, &token); err != nil {
			return selfHostedBarkDeviceIndex{}, err
		}
		key = normalizeBarkDeviceKey(key, caseInsensitive)
		if key != "" {
			devices[key] = strings.TrimSpace(token) != ""
		}
	}
	if err := rows.Err(); err != nil {
		return selfHostedBarkDeviceIndex{}, err
	}
	return selfHostedBarkDeviceIndex{devices: devices, caseInsensitive: caseInsensitive}, nil
}
