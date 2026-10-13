// Command worker simulates provisioning: it consumes tasks from the "tasks"
// queue and, for each, publishes in_progress, waits a random delay, then
// publishes done or failed on "task-updates". See internal/worker.
//
// Every flag can also be set by environment variable (flag wins):
//
//	--amqp-url       AMQP_URL
//	--fail-rate      WORKER_FAIL_RATE      probability 0..1 that a task fails
//	--min-delay-ms   WORKER_MIN_DELAY_MS   minimum simulated work per task
//	--max-delay-ms   WORKER_MAX_DELAY_MS   maximum simulated work per task
//	--concurrency    WORKER_CONCURRENCY    tasks processed at once
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"os"
	"os/signal"
	"strconv"
	"syscall"
	"time"

	"controlplane/internal/worker"
)

type config struct {
	AMQPURL     string
	FailRate    float64
	MinDelayMS  int
	MaxDelayMS  int
	Concurrency int
}

func main() {
	if err := run(); err != nil {
		slog.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run() error {
	slog.SetDefault(slog.New(slog.NewJSONHandler(os.Stdout, nil)))

	cfg, err := loadConfig()
	if err != nil {
		return err
	}

	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	if cfg.AMQPURL == "" {
		return errors.New("AMQP_URL (or --amqp-url) is required")
	}
	slog.Info("worker started",
		"fail_rate", cfg.FailRate, "min_delay_ms", cfg.MinDelayMS, "max_delay_ms", cfg.MaxDelayMS,
		"concurrency", cfg.Concurrency) // never log the AMQP URL: it contains credentials

	// Blocks until ctx is cancelled (Ctrl-C / docker stop), reconnecting to
	// RabbitMQ as needed, and lets in-flight tasks finish or requeue.
	worker.New(worker.Config{
		AMQPURL:     cfg.AMQPURL,
		FailRate:    cfg.FailRate,
		MinDelay:    time.Duration(cfg.MinDelayMS) * time.Millisecond,
		MaxDelay:    time.Duration(cfg.MaxDelayMS) * time.Millisecond,
		Concurrency: cfg.Concurrency,
	}).Run(ctx)

	slog.Info("worker stopped")
	return nil
}

// loadConfig reads env vars as defaults, then lets flags override them.
func loadConfig() (config, error) {
	var cfg config
	failRate, err := envFloat("WORKER_FAIL_RATE", 0)
	if err != nil {
		return cfg, err
	}
	minDelay, err := envInt("WORKER_MIN_DELAY_MS", 500)
	if err != nil {
		return cfg, err
	}
	maxDelay, err := envInt("WORKER_MAX_DELAY_MS", 3000)
	if err != nil {
		return cfg, err
	}
	concurrency, err := envInt("WORKER_CONCURRENCY", 10)
	if err != nil {
		return cfg, err
	}

	flag.StringVar(&cfg.AMQPURL, "amqp-url", os.Getenv("AMQP_URL"), "RabbitMQ connection URL")
	flag.Float64Var(&cfg.FailRate, "fail-rate", failRate, "probability (0..1) that a task fails")
	flag.IntVar(&cfg.MinDelayMS, "min-delay-ms", minDelay, "minimum simulated work per task, ms")
	flag.IntVar(&cfg.MaxDelayMS, "max-delay-ms", maxDelay, "maximum simulated work per task, ms")
	flag.IntVar(&cfg.Concurrency, "concurrency", concurrency, "tasks processed at once")
	flag.Parse()

	switch {
	case cfg.FailRate < 0 || cfg.FailRate > 1:
		return cfg, fmt.Errorf("fail-rate must be between 0 and 1, got %v", cfg.FailRate)
	case cfg.MinDelayMS < 0 || cfg.MaxDelayMS < cfg.MinDelayMS:
		return cfg, fmt.Errorf("need 0 <= min-delay-ms <= max-delay-ms, got %d..%d", cfg.MinDelayMS, cfg.MaxDelayMS)
	case cfg.Concurrency < 1:
		return cfg, fmt.Errorf("concurrency must be >= 1, got %d", cfg.Concurrency)
	}
	return cfg, nil
}

func envFloat(key string, def float64) (float64, error) {
	s := os.Getenv(key)
	if s == "" {
		return def, nil
	}
	v, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}

func envInt(key string, def int) (int, error) {
	s := os.Getenv(key)
	if s == "" {
		return def, nil
	}
	v, err := strconv.Atoi(s)
	if err != nil {
		return 0, fmt.Errorf("%s: %w", key, err)
	}
	return v, nil
}