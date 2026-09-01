package main

import (
	"context"
	"flag"
	"log"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/trilochanparida/go-pipeline/internal/api"
	"github.com/trilochanparida/go-pipeline/internal/pipeline"
	"github.com/trilochanparida/go-pipeline/internal/store"
)

func main() {
	port := flag.String("port", "8080", "Port to listen on")
	dbPath := flag.String("db", "pipeline.db", "SQLite database file path")
	flag.Parse()

	if envPort := os.Getenv("PORT"); envPort != "" {
		*port = envPort
	}
	if envDB := os.Getenv("DB_PATH"); envDB != "" {
		*dbPath = envDB
	}

	log.Println("Starting Data Processing Pipeline Server...")

	// Initialize SQLite Store
	dbStore, err := store.NewSQLiteStore(*dbPath)
	if err != nil {
		log.Fatalf("Failed to initialize database: %v", err)
	}
	defer dbStore.Close()
	log.Printf("Database initialized at %s", *dbPath)

	// Initialize Pipeline Manager
	mgr := pipeline.NewManager(dbStore)

	// Initialize HTTP Handlers and Router
	handler := api.NewHandler(mgr)
	router := api.NewRouter(handler)

	// Create and Start Server
	srv := api.NewServer(":"+*port, router)

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		if err := srv.Start(); err != nil {
			log.Fatalf("Server error: %v", err)
		}
	}()

	<-stop
	log.Println("Received termination signal, shutting down...")

	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()

	if err := srv.Shutdown(ctx); err != nil {
		log.Fatalf("Server forced to shutdown: %v", err)
	}

	log.Println("Server gracefully stopped.")
}
