// Command worker simulates provisioning: it will consume tasks from the
// broker, report in_progress, wait, then report done or failed.
//
// Current stage: connects to RabbitMQ and declares the queues, then waits
// for a shutdown signal; it does not process tasks yet.
//
// Every flag can also be set by environment variable (flag wins):
//
//	--amqp-url       AMQP_URL
//	--fail-rate      WORKER_FAIL_RATE      probability 0..1 that a task fails
//	--min-delay-ms   WORKER_MIN_DELAY_MS   minimum simulated work per step
//	--max-delay-ms   WORKER_MAX_DELAY_MS   maximum simulated work per step
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

	"controlplane/internal/broker"
)

type config struct {
	AMQPURL    string
	FailRate   float64
	MinDelayMS int
	MaxDelayMS int
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

	slog.Info("worker started",
		"fail_rate", cfg.FailRate, "min_delay_ms", cfg.MinDelayMS, "max_delay_ms", cfg.MaxDelayMS,
		"amqp_configured", cfg.AMQPURL != "") // never log the URL itself: it contains credentials

	if cfg.AMQPURL == "" {
		return errors.New("AMQP_URL (or --amqp-url) is required")
	}
	// The worker can't do anything without the broker, so it blocks here
	// (retrying with backoff) until RabbitMQ is reachable.
	conn, _, err := broker.Setup(ctx, cfg.AMQPURL)
	if err != nil {
		if ctx.Err() != nil {
			return nil // shut down while waiting for the broker
		}
		return err
	}
	defer conn.Close()

	// TODO (later steps):
	//   - broker.Consume(ch, broker.QueueTasks, ...); for each task:
	//       publish {update_id, task_id, in_progress} to broker.QueueTaskUpdates
	//       sleep random(min, max)
	//       publish done or failed (fail-rate) to broker.QueueTaskUpdates
	//       ack the task
	slog.Info("worker connected; task processing not implemented yet")

	<-ctx.Done()
	slog.Info("worker shutting down")
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

	flag.StringVar(&cfg.AMQPURL, "amqp-url", os.Getenv("AMQP_URL"), "RabbitMQ connection URL")
	flag.Float64Var(&cfg.FailRate, "fail-rate", failRate, "probability (0..1) that a task fails")
	flag.IntVar(&cfg.MinDelayMS, "min-delay-ms", minDelay, "minimum delay per step, ms")
	flag.IntVar(&cfg.MaxDelayMS, "max-delay-ms", maxDelay, "maximum delay per step, ms")
	flag.Parse()

	switch {
	case cfg.FailRate < 0 || cfg.FailRate > 1:
		return cfg, fmt.Errorf("fail-rate must be between 0 and 1, got %v", cfg.FailRate)
	case cfg.MinDelayMS < 0 || cfg.MaxDelayMS < cfg.MinDelayMS:
		return cfg, fmt.Errorf("need 0 <= min-delay-ms <= max-delay-ms, got %d..%d", cfg.MinDelayMS, cfg.MaxDelayMS)
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