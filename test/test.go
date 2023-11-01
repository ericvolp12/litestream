package main

import (
	"context"
	"database/sql"
	"fmt"
	"log"
	"sync"
	"time"

	_ "github.com/mattn/go-sqlite3"
	"golang.org/x/sync/semaphore"
)

const (
	DBPrefix         = "dbs/testdb_"
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

	db, err := sql.Open("sqlite3", dbName)
	if err != nil {
		log.Fatalf("Failed to open database: %v", err)
	}
	defer db.Close()
	defer sem.Release(1)

	_, err = db.Exec(`PRAGMA journal_mode=WAL;
                      CREATE TABLE IF NOT EXISTS test_table (id INTEGER PRIMARY KEY, timestamp DATETIME DEFAULT CURRENT_TIMESTAMP);`)
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

		_, err = db.Exec("INSERT INTO test_table (timestamp) VALUES (datetime('now'));")
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
		go initializeDB(fmt.Sprintf("%s%d.sqlite", DBPrefix, i), &wg)
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
				go updateDB(fmt.Sprintf("%s%d.sqlite", DBPrefix, i), &wg)
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
