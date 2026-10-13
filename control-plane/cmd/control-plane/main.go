// Command control-plane runs the HTTP API.
//
// Current stage: the API reads and writes tenants and tasks in Postgres.
// Tasks are written to the outbox (published_at NULL) but not yet published:
// the outbox relay comes next.
//
// Needs DATABASE_URL (compose sets it). AMQP_URL is optional for now.
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
	"syscall"
	"time"

	"controlplane/internal/api"
	"controlplane/internal/broker"
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

	// RabbitMQ: connect and declare queues in the background, so the API is
	// up even while the broker is still starting (or down).
	if url := os.Getenv("AMQP_URL"); url != "" {
		go func() {
			conn, _, err := broker.Setup(ctx, url)
			if err != nil {
				if ctx.Err() == nil {
					slog.Error("rabbitmq setup failed", "err", err)
				}
				return
			}
			<-ctx.Done()
			_ = conn.Close()
		}()
	} else {
		slog.Warn("AMQP_URL not set; running without RabbitMQ")
	}

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

	// TODO (later steps):
	//   - start outbox relay goroutine            (tasks → broker, uses broker.Publisher)
	//   - start task update consumer goroutine    (task-updates → DB, uses broker.Consume)

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewHandler(svc, ready),
		ReadHeaderTimeout: 5 * time.Second,
	}

	// Provided by compose; not used until the store and broker exist. Log
	// only whether they are set: the URLs contain credentials.
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
	return srv.Shutdown(shutdownCtx)
}