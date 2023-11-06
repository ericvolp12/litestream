package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"fmt"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sync/semaphore"
)

const (
	PathPrefix       = "dbs"
	DBPrefix         = "testdb_"
	DBCount          = 100_000
	ActiveDBCount    = 1_000
	UpdateInterval   = 5 * time.Second
	RotationInterval = 1 * time.Minute
	MaxConcurrency   = 200 // limit the number of simultaneous goroutines
)

var sem = semaphore.NewWeighted(MaxConcurrency)

// initializeDB creates an SQLite database and sets up a test table
func initializeDB(dbName string, wg *sync.WaitGroup) {
	defer wg.Done()

	// Acquire a slot from the semaphore
	sem.Acquire(context.Background(), 1)

	// Create parent directories if they don't exist
	if err := os.MkdirAll(filepath.Dir(dbName), 0755); err != nil {
		log.Fatalf("Failed to create parent directories: %v", err)
	}

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	defer sem.Release(1)

	_, err = db.Exec(`PRAGMA journal_mode=WAL;
					  PRAGMA busy_timeout=5000;
					  PRAGMA wal_autocheckpoint=0;
					  PRAGMA synchronous=NORMAL;
                      CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP, content TEXT);`)
	if err != nil {
		log.Fatalf("Failed to create table: %v", err)
	}
}

// updateDB inserts a new record with the current timestamp into the test table
func updateDB(dbName string, wg *sync.WaitGroup) {
	// Acquire a slot from the semaphore
	sem.Acquire(context.Background(), 1)

	go func() {
		defer func() {
			sem.Release(1)
			wg.Done()
		}()

		db, err := sql.Open("sqlite3", dbName)
		if err != nil {
			log.Fatalf("Failed to open database: %v", err)
		}
		defer db.Close()

		randomBytes := make([]byte, 3000)
		_, err = rand.Read(randomBytes)
		if err != nil {
			log.Fatalf("Failed to generate random content: %v", err)
		}

		randomContent := string(randomBytes)

		_, err = db.Exec("INSERT INTO test_table (timestamp, content) VALUES (datetime('now'), ?);", randomContent)
		if err != nil {
			log.Fatalf("Failed to insert record: %v", err)
		}
	}()
}

func main() {
	var wg sync.WaitGroup

	// Initialize all databases
	for i := 1; i <= DBCount; i++ {
		wg.Add(1)
		go initializeDB(generateDBPath(i), &wg)
	}

	wg.Wait() // wait for all initialization to complete

	startIdx := 1
	for {
		log.Printf("Updating databases from %d to %d", startIdx, startIdx+ActiveDBCount-1)

		// For the rotation period, keep updating the set of databases every 5 seconds
		endTime := time.Now().Add(RotationInterval)
		for time.Now().Before(endTime) {
			for i := startIdx; i < startIdx+ActiveDBCount && i <= DBCount; i++ {
				wg.Add(1)
				go updateDB(generateDBPath(i), &wg)
			}

			wg.Wait() // Wait for all updates to finish
			time.Sleep(UpdateInterval)
		}

		// Move to the next set of databases
		startIdx += ActiveDBCount
		if startIdx > DBCount {
			startIdx = 1
		}
	}
}

func generateDBPath(i int) string {
	return fmt.Sprintf("%s/%d/%s%d.sqlite", PathPrefix, i%100, DBPrefix, i)
}
