package main

import (
	"context"
	"flag"
	"fmt"
	"log/slog"
	"net"
	"net/http"
	_ "net/http/pprof"
	"net/url"
	"os"
	"os/exec"
	"path/filepath"
	"slices"
	"strings"
	"sync"
	"time"

	"github.com/benbjohnson/litestream"
	"github.com/fsnotify/fsnotify"
	"github.com/mattn/go-shellwords"
	"github.com/prometheus/client_golang/prometheus/promhttp"
)

type DBEntry struct {
	DB        *litestream.DB
	ExpiresAt time.Time
}

// ReplicateCommand represents a command that continuously replicates SQLite databases.
type ReplicateDirCommand struct {
	cmd    *exec.Cmd  // subcommand
	execCh chan error // subcommand error channel

	Config Config

	// List of managed databases specified in the config.
	DBs []*litestream.DB

	lk           sync.RWMutex
	DBEntries    map[string]*DBEntry
	DBTTL        time.Duration
	DebounceList []string
}

func NewReplicateDirCommand() *ReplicateDirCommand {
	return &ReplicateDirCommand{
		execCh:    make(chan error),
		DBEntries: make(map[string]*DBEntry),
		DBTTL:     5 * time.Minute,
	}
}

// ParseFlags parses the CLI flags and loads the configuration file.
func (c *ReplicateDirCommand) ParseFlags(ctx context.Context, args []string) (err error) {
	fs := flag.NewFlagSet("litestream-replicate", flag.ContinueOnError)
	fs.Usage = c.Usage
	if err := fs.Parse(args); err != nil {
		return err
	}

	if fs.NArg() < 2 {
		return fmt.Errorf("must specify at least one directory and one replica root URL")
	}

	c.Config.Dirs = append(c.Config.Dirs, fs.Arg(0))
	c.Config.ReplicaRoot = fs.Arg(1)

	return nil
}

// Run loads all databases specified in the configuration.
func (c *ReplicateDirCommand) Run() (err error) {
	// Display version information.
	slog.Info("litestream", "version", Version)

	// Discover databases.
	if len(c.Config.Dirs) == 0 {
		slog.Error("no directories specified in configuration")
		return nil
	}

	// Watch directories for changes in a separate goroutine.
	shutdown := make(chan struct{})
	go func() {
		if err := c.watchDirs(c.Config.Dirs, func(path string) {
			if err := c.syncDB(path); err != nil {
				slog.Error("failed to sync DB", "error", err)
			}
		}, shutdown); err != nil {
			slog.Error("failed to watch directories", "error", err)
		}
	}()

	// Trim expired DBs in a separate goroutine.
	go func() {
		for {
			if err := c.expireDBs(); err != nil {
				slog.Error("failed to expire DBs", "error", err)
			}
			time.Sleep(time.Second * 5)
		}
	}()

	// Serve metrics over HTTP if enabled.
	if c.Config.Addr != "" {
		hostport := c.Config.Addr
		if host, port, _ := net.SplitHostPort(c.Config.Addr); port == "" {
			return fmt.Errorf("must specify port for bind address: %q", c.Config.Addr)
		} else if host == "" {
			hostport = net.JoinHostPort("localhost", port)
		}

		slog.Info("serving metrics on", "url", fmt.Sprintf("http://%s/metrics", hostport))
		go func() {
			http.Handle("/metrics", promhttp.Handler())
			if err := http.ListenAndServe(c.Config.Addr, nil); err != nil {
				slog.Error("cannot start metrics server", "error", err)
			}
		}()
	}

	// Parse exec commands args & start subprocess.
	if c.Config.Exec != "" {
		execArgs, err := shellwords.Parse(c.Config.Exec)
		if err != nil {
			return fmt.Errorf("cannot parse exec command: %w", err)
		}

		c.cmd = exec.Command(execArgs[0], execArgs[1:]...)
		c.cmd.Env = os.Environ()
		c.cmd.Stdout = os.Stdout
		c.cmd.Stderr = os.Stderr
		if err := c.cmd.Start(); err != nil {
			return fmt.Errorf("cannot start exec command: %w", err)
		}
		go func() { c.execCh <- c.cmd.Wait() }()
	}

	return nil
}

// Close closes all open databases.
func (c *ReplicateDirCommand) Close() (err error) {
	err = c.shutdown()
	if err != nil {
		slog.Error("failed to shutdown", "error", err)
	}
	return err
}

// Usage prints the help screen to STDOUT.
func (c *ReplicateDirCommand) Usage() {
	fmt.Printf(`
The replicate command starts a server to monitor & replicate databases. 
You can specify your database & replicas in a configuration file or you can
replicate a single database file by specifying its path and its replicas in the
command line arguments.

Usage:

	litestream replicate [arguments]

	litestream replicate [arguments] DB_PATH REPLICA_URL [REPLICA_URL...]

Arguments:

	-config PATH
	    Specifies the configuration file.
	    Defaults to %s

	-exec CMD
	    Executes a subcommand. Litestream will exit when the child
	    process exits. Useful for simple process management.

	-no-expand-env
	    Disables environment variable expansion in configuration file.

`[1:], DefaultConfigPath())
}

func (c *ReplicateDirCommand) watchDirs(dirs []string, onUpdate func(string), shutdown chan struct{}) error {
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

func (c *ReplicateDirCommand) syncDB(path string) error {
	c.lk.RLock()
	dbEntry, ok := c.DBEntries[path]
	debounceIdx := slices.Index(c.DebounceList, path)
	c.lk.RUnlock()
	if ok {
		c.lk.Lock()
		dbEntry.ExpiresAt = time.Now().Add(c.DBTTL)
		c.lk.Unlock()
		return nil
	}

	if debounceIdx >= 0 {
		c.lk.Lock()
		c.DebounceList = append(c.DebounceList[:debounceIdx], c.DebounceList[debounceIdx+1:]...)
		c.lk.Unlock()
		return nil
	}

	slog.Info("syncing new DB", "path", path)

	ep, err := url.Parse(c.Config.ReplicaRoot)
	if err != nil {
		return fmt.Errorf("failed to parse replica root URL (%s): %w", c.Config.ReplicaRoot, err)
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

	c.lk.Lock()
	c.DBEntries[path] = &DBEntry{
		DB:        db,
		ExpiresAt: time.Now().Add(c.DBTTL),
	}
	c.lk.Unlock()

	return nil
}

func (c *ReplicateDirCommand) expireDBs() error {
	c.lk.Lock()
	defer c.lk.Unlock()

	for path, dbEntry := range c.DBEntries {
		if time.Now().After(dbEntry.ExpiresAt) {
			slog.Info("closing expired DB", "path", path)
			if err := dbEntry.DB.Close(); err != nil {
				return fmt.Errorf("failed to close expired DB (%s): %w", path, err)
			}
			delete(c.DBEntries, path)
			c.DebounceList = append(c.DebounceList, path)
		}
	}

	return nil
}

func (c *ReplicateDirCommand) shutdown() error {
	c.lk.Lock()
	defer c.lk.Unlock()

	slog.Info("shutting down dir replication")

	for _, dbEntry := range c.DBEntries {
		if err := dbEntry.DB.Close(); err != nil {
			slog.Error("failed to close DB", "error", err)
		}
	}

	slog.Info("all DBs closed")

	return nil
}
