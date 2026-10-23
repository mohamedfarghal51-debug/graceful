// Command basic is a runnable graceful example: a small HTTP service with
// production timeouts, SIGINT/SIGTERM handling, in-flight request draining,
// and ordered shutdown hooks.
//
// Run it:
//
//	go run .
//
// Then, in another terminal:
//
//	curl localhost:8080/healthz
//	curl localhost:8080/slow   # takes 3s; press Ctrl+C mid-request and
//	                           # watch the request finish before exit
package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/parsaaftabi/graceful"
)

func main() {
	mux := http.NewServeMux()
	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write([]byte("ok\n"))
	})
	mux.HandleFunc("/slow", func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(3 * time.Second)
		_, _ = w.Write([]byte("slow response finished\n"))
	})

	srv := graceful.New(mux,
		graceful.WithAddr(":8080"),
		graceful.WithOnShutdown(
			func(ctx context.Context) error {
				fmt.Println("hook 1: closing database pool")
				return nil
			},
			func(ctx context.Context) error {
				fmt.Println("hook 2: flushing telemetry")
				return nil
			},
		),
	)

	if err := srv.Run(context.Background()); err != nil {
		log.Fatal(err)
	}
}
