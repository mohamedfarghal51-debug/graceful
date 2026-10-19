# graceful

[![CI](https://github.com/parsaaftabi/graceful/actions/workflows/ci.yml/badge.svg)](https://github.com/parsaaftabi/graceful/actions/workflows/ci.yml)
[![Go Reference](https://pkg.go.dev/badge/github.com/parsaaftabi/graceful.svg)](https://pkg.go.dev/github.com/parsaaftabi/graceful)
[![License: MIT](https://img.shields.io/badge/License-MIT-yellow.svg)](LICENSE)

Production-grade HTTP server lifecycle for Go: sane timeout defaults, SIGINT/SIGTERM handling, in-flight request draining, and ordered shutdown hooks. Zero dependencies.

```go
mux := http.NewServeMux()
mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
    w.WriteHeader(http.StatusOK)
})

srv := graceful.New(mux,
    graceful.WithAddr(":8080"),
    graceful.WithOnShutdown(func(ctx context.Context) error {
        return db.Close() // pools, telemetry, whatever needs cleanup
    }),
)
if err := srv.Run(context.Background()); err != nil {
    log.Fatal(err)
}
```

## Why

Every production Go HTTP server needs the same five things, and the standard library leaves all of them at zero values:

1. **Timeouts** — `http.Server` defaults every timeout to 0 (no timeout). One slow client can hold a connection forever.
2. **Signal handling** — `ListenAndServe` doesn't listen for SIGINT/SIGTERM.
3. **Draining** — killing the process mid-request drops real user traffic.
4. **Cleanup ordering** — DB pools and telemetry need to flush before exit.
5. **A deadline on all of it** — shutdown itself must not hang forever.

`graceful` wires all five in ~200 lines you could read in one sitting.

## Install

```bash
go get github.com/parsaaftabi/graceful
```

Requires Go 1.24+.

## Defaults

| Setting | Default | Why |
|---|---|---|
| Addr | `:8080` | |
| ReadHeaderTimeout | 5s | Slowloris protection: headers must arrive fast |
| ReadTimeout | 10s | Bounds the full request read, body included |
| WriteTimeout | 30s | Bounds the full response write, handler time included |
| IdleTimeout | 120s | Keep-alive connections don't linger forever |
| ShutdownTimeout | 20s | In-flight requests + hooks must finish within this |

Every default is overridable with a `With...` option. If you serve legitimately slow endpoints (large exports, SSE), raise `WriteTimeout` — it's the one people get wrong most often.

## Shutdown hooks

Hooks run in order after the listener stops accepting new connections. Each receives the shutdown context (cancelled at `ShutdownTimeout`). A failing hook is logged and never aborts the remaining hooks — one misbehaving cleanup must not block the rest.

## Behavior notes

- `Run` blocks until the context is cancelled or SIGINT/SIGTERM arrives, then returns `nil` on clean shutdown.
- `http.ErrServerClosed` is treated as a clean exit, never an error.
- `WithTLS` requires both cert and key; passing only one is a configuration error returned by `Run`.
- `HTTPServer()` exposes the underlying `*http.Server` for advanced tweaks (custom `ConnState`, HTTP/2 config).

## License

MIT — see [LICENSE](LICENSE).
