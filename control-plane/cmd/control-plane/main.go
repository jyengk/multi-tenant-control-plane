// Command control-plane runs the HTTP API.
//
// One process, three components:
//   - HTTP API: reads/writes tenants and tasks in Postgres
//   - outbox relay: publishes committed tasks to the "tasks" queue
//   - task update consumer: applies worker progress from "task-updates"
//
// Needs DATABASE_URL (compose sets it). Without AMQP_URL, only the API runs.
//
//	DATABASE_URL=postgres://controlplane:controlplane@localhost:5432/controlplane?sslmode=disable \
//	  go run ./cmd/control-plane
package main

import (
	"context"
	"errors"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"sync"
	"syscall"
	"time"

	"controlplane/internal/api"
	"controlplane/internal/consumer"
	"controlplane/internal/outbox"
	"controlplane/internal/store"
)

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	addr := flag.String("addr", ":8080", "HTTP listen address")
	flag.Parse()

	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	// Cancelled on Ctrl-C or SIGTERM (docker stop).
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// Postgres is required: every API call reads or writes it. Connect and
	// apply the embedded schema BEFORE serving, so no request can ever hit a
	// database without its tables.
	dbURL := os.Getenv("DATABASE_URL")
	if dbURL == "" {
		return errors.New("DATABASE_URL is required")
	}
	pool, err := store.Open(ctx, dbURL)
	if err != nil {
		if ctx.Err() != nil {
			return nil // shut down while waiting for the database
		}
		return err
	}
	defer pool.Close()

	// The Postgres store implements api.Service: this is what the handlers'
	// h.svc.CreateTenant(...) etc. call.
	svc := store.New(pool)
	ready := api.ReadyFunc(pool.Ping) // /readyz reports 503 if the database goes away

	// Outbox relay: publishes committed tasks to RabbitMQ. It connects (and
	// reconnects) in the background, so the API is up even while the broker
	// is starting or down; tasks simply wait in the outbox meanwhile.
	var wg sync.WaitGroup
	if amqpURL := os.Getenv("AMQP_URL"); amqpURL != "" {
		relay := outbox.New(pool, amqpURL, outbox.Config{})
		svc.OnTaskCommitted(relay.Nudge) // publish within ms of commit, not at the next poll
		wg.Add(1)
		go func() {
			defer wg.Done()
			relay.Run(ctx)
		}()

		// Task update consumer: applies worker progress (task-updates) to
		// tasks and tenants. Same lifecycle as the relay.
		updates := consumer.New(svc, amqpURL)
		wg.Add(1)
		go func() {
			defer wg.Done()
			updates.Run(ctx)
		}()
	} else {
		slog.Warn("AMQP_URL not set; tasks stay in the outbox and no updates are consumed")
	}

	srv := &http.Server{
		Addr: *addr,
		// CORS is off unless CORS_ALLOWED_ORIGIN is set (e.g. for Swagger UI).
		Handler:           api.CORS(os.Getenv("CORS_ALLOWED_ORIGIN"), api.NewHandler(svc, ready)),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Log only whether these are set: the URLs contain credentials.
	slog.Info("config",
		"database_configured", os.Getenv("DATABASE_URL") != "",
		"amqp_configured", os.Getenv("AMQP_URL") != "")

	errCh := make(chan error, 1)
	go func() {
		slog.Info("control-plane listening", "addr", *addr)
		errCh <- srv.ListenAndServe()
	}()

	select {
	case err := <-errCh:
		if errors.Is(err, http.ErrServerClosed) {
			return nil
		}
		return err
	case <-ctx.Done():
	}

	slog.Info("shutting down")
	shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	err = srv.Shutdown(shutdownCtx) // finish in-flight requests
	wg.Wait()                       // let the relay and consumer finish their current work
	return err
}