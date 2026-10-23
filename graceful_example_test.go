package graceful_test

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/parsaaftabi/graceful"
)

// ExampleNew shows the minimal production setup: a mux, production timeout
// defaults, signal handling, request draining, and one shutdown hook that
// closes a resource (here, a stand-in for a database pool).
func ExampleNew() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
	})

	srv := graceful.New(mux,
		graceful.WithAddr(":8080"),
		graceful.WithOnShutdown(func(ctx context.Context) error {
			// Close database pools, flush telemetry, release resources.
			fmt.Println("cleanup done")
			return nil
		}),
	)

	// Run blocks until SIGINT/SIGTERM or ctx cancellation, then drains
	// in-flight requests and runs the shutdown hooks.
	_ = srv
}

// ExampleServer_customTimeouts shows overriding the defaults for a service
// with legitimately slow endpoints (large exports, streaming responses).
func ExampleServer_customTimeouts() {
	mux := http.NewServeMux()

	srv := graceful.New(mux,
		graceful.WithAddr(":8080"),
		graceful.WithWriteTimeout(5*time.Minute), // slow export endpoint
		graceful.WithShutdownTimeout(30*time.Second),
	)

	if err := srv.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
