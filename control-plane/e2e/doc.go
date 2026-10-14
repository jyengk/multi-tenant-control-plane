// Package e2e holds black-box end-to-end tests that run against a live stack
// (docker compose up). They talk to the system only through its public
// interfaces: the REST API and RabbitMQ.
//
// The tests are behind the "e2e" build tag, so a plain `go test ./...` skips
// them. Run them with scripts/e2e.sh, or directly:
//
//	go test -tags e2e -count=1 -v ./e2e/
//
// Two groups, chosen by whether the worker simulator is running:
//
//	worker running  → Lifecycle*, APIErrors*  (the real worker drives tasks)
//	worker stopped  → Matrix*, Messaging*, Concurrency*  (the test plays the
//	                   worker by publishing updates, so outcomes are exact)
//
// Each test skips itself, with a message, if the worker is in the wrong
// state. scripts/e2e.sh runs both groups in the right order.
//
// Addresses (defaults suit docker compose on localhost):
//
//	E2E_API_URL   http://localhost:8080
//	E2E_AMQP_URL  amqp://controlplane:controlplane@localhost:5672/
package e2e
