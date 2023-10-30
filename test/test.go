package main

import (
	"database/sql"
	"fmt"
	"log"
	"time"

	_ "github.com/mattn/go-sqlite3"
)

const (
	DBPrefix         = "dbs/testdb_"
	DBCount          = 100
	ActiveDBCount    = 10
	UpdateInterval   = 5 * time.Second
	RotationInterval = 3 * time.Minute
)

// initializeDB creates an SQLite database and sets up a test table
func initializeDB(dbName string) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec(`PRAGMA journal_mode=WAL;
                      CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

// updateDB inserts a new record with the current timestamp into the test table
func updateDB(dbName string) {
	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()

	_, err = db.Exec("INSERT INTO test_table (timestamp) VALUES (datetime('now'));")
	if err != nil {
		log.Fatalf("Failed to insert record: %v", err)
	}
}

func main() {
	// Initialize all databases
	for i := 1; i <= DBCount; i++ {
		initializeDB(fmt.Sprintf("%s%d.sqlite", DBPrefix, i))
	}

	startIdx := 1
	for {
		log.Printf("Updating databases from %d to %d", startIdx, startIdx+ActiveDBCount-1)

		// For the rotation period, keep updating the set of databases every 5 seconds
		endTime := time.Now().Add(RotationInterval)
		for time.Now().Before(endTime) {
			for i := startIdx; i < startIdx+ActiveDBCount && i <= DBCount; i++ {
				updateDB(fmt.Sprintf("%s%d.sqlite", DBPrefix, i))
			}
			time.Sleep(UpdateInterval)
		}

		// Move to the next set of databases
		startIdx += ActiveDBCount
		if startIdx > DBCount {
			startIdx = 1
		}
	}
}
