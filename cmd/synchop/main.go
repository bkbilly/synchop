package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"strconv"
	"strings"
	"syscall"
	"time"

	"synchop/internal/config"
	"synchop/internal/engine"
	"synchop/internal/history"
	"synchop/internal/web"
)

type cliBroadcaster struct {
	lastPhase string
}

func (c *cliBroadcaster) BroadcastState(st engine.SyncState) {
	if st.CurrentPhase != "" && st.CurrentPhase != c.lastPhase {
		c.lastPhase = st.CurrentPhase
		fmt.Printf("  • [%s] %s\n", strings.ToUpper(string(st.Status)), st.CurrentPhase)
	}
}

func formatBytes(b int64) string {
	const unit = 1024
	if b < unit {
		return fmt.Sprintf("%d B", b)
	}
	div, exp := int64(unit), 0
	for n := b / unit; n >= unit; n /= unit {
		div *= unit
		exp++
	}
	return fmt.Sprintf("%.2f %cB", float64(b)/float64(div), "KMGTPE"[exp])
}

func main() {
	// Native SSH_ASKPASS helper mode (called by OpenSSH to fetch password)
	if os.Getenv("SYNCHOP_IS_ASKPASS") == "1" {
		fmt.Println(os.Getenv("SYNCHOP_SSH_PASS"))
		return
	}

	// Detect standalone one-shot sync command (e.g. `synchop sync` or `synchop -sync`)
	isSyncOnly := false
	var filteredArgs []string
	for i, arg := range os.Args {
		if i == 0 {
			filteredArgs = append(filteredArgs, arg)
			continue
		}
		if arg == "sync" || arg == "run" || arg == "-sync" || arg == "--sync" || arg == "-run" || arg == "--run" {
			isSyncOnly = true
		} else {
			filteredArgs = append(filteredArgs, arg)
		}
	}

	flags := flag.NewFlagSet("synchop", flag.ExitOnError)
	var configPath string
	var port int
	flags.StringVar(&configPath, "config", "", "Path to config file (default: config.json or /data/config.json)")
	flags.IntVar(&port, "port", 0, "HTTP server port (overrides config and PORT env var)")
	_ = flags.Parse(filteredArgs[1:])

	// Check environment variable overrides if not provided via flags
	if port <= 0 {
		if envPort := os.Getenv("PORT"); envPort != "" {
			if p, err := strconv.Atoi(envPort); err == nil && p > 0 {
				port = p
			}
		}
	}

	if configPath == "" {
		if envConfig := os.Getenv("CONFIG_PATH"); envConfig != "" {
			configPath = envConfig
		} else if envData := os.Getenv("DATA_DIR"); envData != "" {
			configPath = filepath.Join(envData, "config.json")
		} else if fi, err := os.Stat("/data"); err == nil && fi.IsDir() {
			configPath = "/data/config.json"
		} else {
			configPath = "config.json"
		}
	}

	cwd, err := os.Getwd()
	if err != nil {
		cwd = "."
	}

	absConfig := configPath
	if !filepath.IsAbs(absConfig) {
		absConfig = filepath.Join(cwd, configPath)
	}

	baseDir := filepath.Dir(absConfig)
	store, err := config.LoadStore(absConfig, baseDir)
	if err != nil {
		log.Fatalf("Failed to load configuration: %v", err)
	}

	historyPath := filepath.Join(filepath.Dir(absConfig), "history.json")
	historyStore, err := history.NewStore(historyPath)
	if err != nil {
		log.Fatalf("Failed to initialize history store: %v", err)
	}

	// -----------------------------------------------------------------
	// Standalone One-Shot CLI Sync (No Web UI)
	// -----------------------------------------------------------------
	if isSyncOnly {
		cliHub := &cliBroadcaster{}
		eng := engine.NewEngine(store, historyStore, cliHub)

		fmt.Println("==================================================================")
		fmt.Println("  ⚡ SyncHop — Standalone Sync Mode")
		fmt.Printf("  • Config File: %s\n", absConfig)
		fmt.Printf("  • Sources:     %d configured\n", len(store.Config.Sync.Sources))
		fmt.Printf("  • Destination: %s (%s)\n", store.Config.Sync.Destination.Name, store.Config.Sync.Destination.RemotePath)
		fmt.Println("==================================================================")

		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		sigCh := make(chan os.Signal, 1)
		signal.Notify(sigCh, os.Interrupt, syscall.SIGTERM)
		go func() {
			<-sigCh
			fmt.Println("\nReceived interrupt signal, cancelling sync...")
			cancel()
		}()

		err = eng.RunSyncBlocking(ctx, "cli")
		st := eng.GetState()
		fmt.Println("------------------------------------------------------------------")
		if err != nil {
			fmt.Printf("✗ Sync Failed: %v\n", err)
			os.Exit(1)
		}

		fmt.Printf("✓ Sync Completed Successfully in %ds\n", st.ElapsedSec)
		summary := historyStore.GetSummary()
		if summary.LastRun != nil {
			fmt.Printf("  • Files Changed:     %d\n", summary.LastRun.FilesChanged)
			fmt.Printf("  • Send Size (Delta): %s\n", formatBytes(summary.LastRun.SendSize))
			fmt.Printf("  • Total Size:        %s\n", formatBytes(summary.LastRun.TotalSize))
			if len(summary.LastRun.Files) > 0 {
				fmt.Println("  • Files Sent to Destination:")
				for i, f := range summary.LastRun.Files {
					if i >= 15 {
						fmt.Printf("    ... and %d more files\n", len(summary.LastRun.Files)-15)
						break
					}
					fmt.Printf("    [%s] %s (%s)\n", f.Action, f.Path, formatBytes(f.Size))
				}
			} else {
				fmt.Println("  • Files Sent:        0 (1:1 mirror already up-to-date)")
			}
		} else {
			fmt.Printf("  • Total Transferred: %s\n", formatBytes(st.TotalBytesTransferred))
		}
		fmt.Println("==================================================================")
		return
	}

	// -----------------------------------------------------------------
	// Full Web Server & Scheduled Daemon Mode
	// -----------------------------------------------------------------
	listenPort := store.Config.HTTPPort
	if port > 0 {
		listenPort = port
	}

	hub := web.NewHub()
	eng := engine.NewEngine(store, historyStore, hub)

	// Start schedule runner (sleeps when idle, 0 background overhead)
	eng.StartScheduler()

	srvHandler := web.NewServer(store, eng, hub)
	addr := fmt.Sprintf(":%d", listenPort)
	srv := &http.Server{
		Addr:    addr,
		Handler: srvHandler,
	}

	fmt.Println("==================================================================")
	fmt.Println("  ⚡ SyncHop — Scheduled Mirroring Engine")
	fmt.Printf("  • Web UI:      http://localhost:%d\n", listenPort)
	fmt.Printf("  • Config File: %s\n", absConfig)
	fmt.Printf("  • Zero Idle:   No background processes active until run\n")
	fmt.Printf("  • CLI Mode:    Run './synchop sync' to execute without web UI\n")
	fmt.Println("==================================================================")

	// Graceful shutdown
	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("HTTP server failed: %v", err)
		}
	}()

	<-stop
	fmt.Println("\nShutting down SyncHop gracefully...")

	shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
	fmt.Println("SyncHop stopped cleanly.")
}
