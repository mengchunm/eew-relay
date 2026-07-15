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
	dsn = strings.TrimSpace(dsn)
	value, ok := barkMySQLPools.Load(dsn)
	if !ok {
		db, err := sql.Open("mysql", dsn)
		if err != nil {
			return false, err
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
		return false, fmt.Errorf("invalid Bark MySQL pool")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()
	var exists bool
	if err := db.QueryRowContext(ctx, `SELECT EXISTS(SELECT 1 FROM devices WHERE `+"`key`"+`=? LIMIT 1)`, barkID).Scan(&exists); err != nil {
		return false, err
	}
	return exists, nil
}
