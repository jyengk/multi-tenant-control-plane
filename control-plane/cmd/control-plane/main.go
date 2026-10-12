// Command control-plane runs the HTTP API.
//
// Current stage: routes and middleware only. Handlers return 501 until they
// are implemented; nothing is persisted and no broker is used.
//
//	go run ./cmd/control-plane              # listens on :8080
//	go run ./cmd/control-plane --addr=:9090
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

	// TODO (later steps):
	//   - connect to Postgres, run migrations     → store implementing api.Service
	//   - connect to RabbitMQ                     → broker
	//   - start outbox relay goroutine            (tasks → broker)
	//   - start progress consumer goroutine       (worker updates → DB)
	var svc api.Service // nil until the store exists; handlers don't use it yet

	srv := &http.Server{
		Addr:              *addr,
		Handler:           api.NewHandler(svc, nil),
		ReadHeaderTimeout: 5 * time.Second,
	}

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
