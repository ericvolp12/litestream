package main

import (
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/fsnotify/fsnotify"
	"github.com/prometheus/client_golang/prometheus/promhttp"
	"github.com/urfave/cli/v2"
)

type DBEntry struct {
	DB        *litestream.DB
	ExpiresAt time.Time
}

type Config struct {
	// List of directories to monitor
	Dirs []string
	// Root URL for directory replication
	ReplicaRoot string
	// Address for metrics server
	Addr string
}

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type Replicator struct {
	Config Config

	lk          sync.RWMutex
	DBEntries   map[string]*DBEntry
	DBTTL       time.Duration
	DebounceSet map[string]time.Time
}

func NewReplicator() *Replicator {
	return &Replicator{
		DBEntries:   make(map[string]*DBEntry),
		DBTTL:       5 * time.Minute,
		DebounceSet: make(map[string]time.Time),
	}
}

func main() {
	r := NewReplicator()
	app := &cli.App{
		Name:  "dirstream",
		Usage: "Replicate SQLite databases in a directory to S3",
		Flags: []cli.Flag{
			&cli.StringSliceFlag{
				Name:     "dir",
				Usage:    "Directories to monitor (can be specified multiple times)",
				EnvVars:  []string{"DIRSTREAM_DIR"},
				Required: true,
			},
			&cli.StringFlag{
				Name:     "replica-root",
				Usage:    "S3 Bucket URL for Replication (https://{s3_url}.com/{bucket_name})",
				EnvVars:  []string{"DIRSTREAM_REPLICA_ROOT"},
				Required: true,
			},
			&cli.StringFlag{
				Name:    "addr",
				Usage:   "Address to serve metrics on",
				EnvVars: []string{"DIRSTREAM_ADDR"},
				Value:   "0.0.0.0:9032",
			},
		},
		Action: r.Run,
	}

	if err := app.Run(os.Args); err != nil {
		slog.Error("failed to run", "error", err)
	}

	slog.Info("dirstream exiting")
}

// Run loads all databases specified in the configuration.
func (r *Replicator) Run(cctx *cli.Context) (err error) {
	// Display version information.
	slog.Info("dirstream starting up")

	// Load configuration.
	r.Config.Dirs = cctx.StringSlice("dir")
	r.Config.ReplicaRoot = cctx.String("replica-root")
	r.Config.Addr = cctx.String("addr")

	// Discover databases.
	if len(r.Config.Dirs) == 0 {
		slog.Error("no directories specified in configuration")
		return nil
	}

	// Watch directories for changes in a separate goroutine.
	shutdown := make(chan struct{})
	go func() {
		if err := r.watchDirs(r.Config.Dirs, func(path string) {
			if err := r.syncDB(path); err != nil {
				slog.Error("failed to sync DB", "error", err)
			}
		}, shutdown); err != nil {
			slog.Error("failed to watch directories", "error", err)
		}
	}()

	// Trim expired DBs in a separate goroutine.
	go func() {
		for {
			if err := r.expireDBs(); err != nil {
				slog.Error("failed to expire DBs", "error", err)
			}
			time.Sleep(time.Second * 5)
		}
	}()

	// Serve metrics over HTTP if enabled.
	if r.Config.Addr != "" {
		hostport := r.Config.Addr
		if host, port, _ := net.SplitHostPort(r.Config.Addr); port == "" {
			return fmt.Errorf("must specify port for bind address: %q", r.Config.Addr)
		} else if host == "" {
			hostport = net.JoinHostPort("localhost", port)
		}

		slog.Info("serving metrics on", "url", fmt.Sprintf("http://%s/metrics", hostport))
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(r.Config.Addr, nil); err != nil {
				slog.Error("cannot start metrics server", "error", err)
			}
		}()
	}

	// Trap SIGINT to trigger a shutdown.
	signals := make(chan os.Signal, 1)
	signal.Notify(signals, syscall.SIGINT, syscall.SIGTERM)

	select {
	case <-signals:
		slog.Info("shutting down on signal")
	}

	slog.Info("shutting down, waiting for workers to clean up...")

	// Close the watcher
	close(shutdown)

	// Sync and close all active DBs
	r.Close()

	slog.Info("all workers shutdown")
	return nil
}

// Close closes all open databases.
func (r *Replicator) Close() (err error) {
	err = r.shutdown()
	if err != nil {
		slog.Error("failed to shutdown", "error", err)
	}
	return err
}

func (r *Replicator) watchDirs(dirs []string, onUpdate func(string), shutdown chan struct{}) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}
	defer watcher.Close()

	log := slog.With("source", "watcher")

	go func() {
		for {
			select {
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				// Only sync on file creation or modification of non `.` prefixed files with `.sqlite` suffix.
				if (event.Op&fsnotify.Write == fsnotify.Write ||
					event.Op&fsnotify.Create == fsnotify.Create) &&
					!strings.HasPrefix(filepath.Base(event.Name), ".") &&
					strings.HasSuffix(event.Name, ".sqlite") {
					log.Info("file modified", "filename", event.Name)
					onUpdate(event.Name)
				}
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Error("watcher error", "error", err)
			}
		}
	}()

	// Add initial directories to the watcher.
	for _, dir := range dirs {
		// Resolve directory path.
		if dir, err = filepath.Abs(dir); err != nil {
			return err
		}
		err = filepath.WalkDir(dir, func(path string, d os.DirEntry, err error) error {
			if err != nil {
				return err
			}

			if d.IsDir() {
				return watcher.Add(path)
			}
			return nil
		})
		if err != nil {
			return err
		}
	}

	<-shutdown
	return nil
}

func (r *Replicator) syncDB(path string) error {
	r.lk.RLock()
	dbEntry, ok := r.DBEntries[path]
	debounceUntil, debounce := r.DebounceSet[path]
	r.lk.RUnlock()
	if ok {
		r.lk.Lock()
		dbEntry.ExpiresAt = time.Now().Add(r.DBTTL)
		r.lk.Unlock()
		return nil
	}

	if debounce {
		if time.Now().Before(debounceUntil) {
			return nil
		}
		r.lk.Lock()
		delete(r.DebounceSet, path)
		r.lk.Unlock()
		return nil
	}

	slog.Info("syncing new DB", "path", path)

	ep, err := url.Parse(r.Config.ReplicaRoot)
	if err != nil {
		return fmt.Errorf("failed to parse replica root URL (%s): %w", r.Config.ReplicaRoot, err)
	}

	dbConfig := DBConfig{Path: path}
	syncInterval := time.Second * 5
	dbConfig.Replicas = append(dbConfig.Replicas, &ReplicaConfig{
		Type:         "s3",
		Endpoint:     fmt.Sprintf("%s://%s", ep.Scheme, ep.Host),
		Bucket:       strings.TrimPrefix(ep.Path, "/"),
		Path:         filepath.Base(path),
		SyncInterval: &syncInterval,
	})

	db, err := NewDBFromConfig(&dbConfig)
	if err != nil {
		return fmt.Errorf("failed to init DB from config for (%s): %w", path, err)
	}

	if err := db.Open(); err != nil {
		return fmt.Errorf("failed to open DB for sync (%s): %w", path, err)
	}

	r.lk.Lock()
	r.DBEntries[path] = &DBEntry{
		DB:        db,
		ExpiresAt: time.Now().Add(r.DBTTL),
	}
	r.lk.Unlock()

	return nil
}

func (r *Replicator) expireDBs() error {
	r.lk.Lock()
	defer r.lk.Unlock()

	for path, dbEntry := range r.DBEntries {
		if time.Now().After(dbEntry.ExpiresAt) {
			slog.Info("closing expired DB", "path", path)
			if err := dbEntry.DB.Close(); err != nil {
				return fmt.Errorf("failed to close expired DB (%s): %w", path, err)
			}
			delete(r.DBEntries, path)
			r.DebounceSet[path] = time.Now().Add(time.Second * 3)
		}
	}

	return nil
}

func (r *Replicator) shutdown() error {
	r.lk.Lock()
	defer r.lk.Unlock()

	slog.Info("shutting down dir replication")

	for _, dbEntry := range r.DBEntries {
		if err := dbEntry.DB.Close(); err != nil {
			slog.Error("failed to close DB", "error", err)
		}
	}

	slog.Info("all DBs closed")

	return nil
}
