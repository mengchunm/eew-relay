package main

import (
	"database/sql"
	"flag"
	"fmt"
	"io"
	"log"
	"os"
	"strings"

	_ "github.com/go-sql-driver/mysql"
	bolt "go.etcd.io/bbolt"
)

func main() {
	source := flag.String("source", "bark.db", "source Bark bbolt database")
	dsn := flag.String("dsn", "", "destination MySQL DSN")
	dryRun := flag.Bool("dry-run", false, "count and validate records without writing")
	flag.Parse()
	if strings.TrimSpace(*dsn) == "" && !*dryRun {
		log.Fatal("-dsn is required unless -dry-run is used")
	}
	snapshot, err := os.CreateTemp("", "bark-migrate-*.db")
	if err != nil {
		log.Fatal(err)
	}
	snapshotPath := snapshot.Name()
	defer os.Remove(snapshotPath)
	sourceFile, err := os.Open(*source)
	if err != nil {
		log.Fatal(err)
	}
	if _, err := io.Copy(snapshot, sourceFile); err != nil {
		log.Fatal(err)
	}
	if err := sourceFile.Close(); err != nil {
		log.Fatal(err)
	}
	if err := snapshot.Close(); err != nil {
		log.Fatal(err)
	}
	sourceDB, err := bolt.Open(snapshotPath, 0o400, &bolt.Options{ReadOnly: true})
	if err != nil {
		log.Fatal(err)
	}
	defer sourceDB.Close()
	devices := make(map[string]string)
	invalid := 0
	if err := sourceDB.View(func(tx *bolt.Tx) error {
		bucket := tx.Bucket([]byte("device"))
		if bucket == nil {
			return fmt.Errorf("device bucket not found")
		}
		return bucket.ForEach(func(key, token []byte) error {
			if len(key) == 0 || len(token) == 0 {
				invalid++
				return nil
			}
			devices[string(key)] = string(token)
			return nil
		})
	}); err != nil {
		log.Fatal(err)
	}
	if *dryRun {
		log.Printf("validated Bark device records=%d skipped_invalid=%d", len(devices), invalid)
		return
	}
	mysqlDB, err := sql.Open("mysql", *dsn)
	if err != nil {
		log.Fatal(err)
	}
	defer mysqlDB.Close()
	if err := mysqlDB.Ping(); err != nil {
		log.Fatal(err)
	}
	tx, err := mysqlDB.Begin()
	if err != nil {
		log.Fatal(err)
	}
	defer tx.Rollback()
	if _, err := tx.Exec(`CREATE TABLE IF NOT EXISTS devices (
		id INT UNSIGNED NOT NULL AUTO_INCREMENT,
		` + "`key`" + ` VARCHAR(255) NOT NULL,
		token VARCHAR(255) NOT NULL,
		PRIMARY KEY (id), UNIQUE KEY ` + "`key`" + ` (` + "`key`" + `)
	) ENGINE=InnoDB DEFAULT CHARSET=utf8mb4`); err != nil {
		log.Fatal(err)
	}
	stmt, err := tx.Prepare(`INSERT INTO devices (` + "`key`" + `, token) VALUES (?, ?)
		ON DUPLICATE KEY UPDATE token=VALUES(token)`)
	if err != nil {
		log.Fatal(err)
	}
	defer stmt.Close()
	for key, token := range devices {
		if _, err := stmt.Exec(key, token); err != nil {
			log.Fatal(err)
		}
	}
	if err := tx.Commit(); err != nil {
		log.Fatal(err)
	}
	var destinationCount int
	if err := mysqlDB.QueryRow(`SELECT COUNT(*) FROM devices`).Scan(&destinationCount); err != nil {
		log.Fatal(err)
	}
	if destinationCount < len(devices) {
		log.Fatalf("destination verification failed: source=%d destination=%d", len(devices), destinationCount)
	}
	log.Printf("migrated Bark device records=%d skipped_invalid=%d destination_total=%d", len(devices), invalid, destinationCount)
}
