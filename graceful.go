// Package graceful runs an [http.Server] with sane production timeouts and
// graceful shutdown.
//
// It wires signal handling (SIGINT/SIGTERM), applies timeout defaults that
// protect against slow clients, and drains in-flight requests before exiting.
// Shutdown hooks let you close database pools, flush telemetry, and release
// other resources within the shutdown deadline.
//
// A minimal program:
//
//	mux := http.NewServeMux()
//	mux.HandleFunc("/healthz", func(w http.ResponseWriter, r *http.Request) {
//		w.WriteHeader(http.StatusOK)
//	})
//
//	srv := graceful.New(mux,
//		graceful.WithAddr(":8080"),
//		graceful.WithOnShutdown(func(ctx context.Context) error {
//			return db.Close()
//		}),
//	)
//	if err := srv.Run(context.Background()); err != nil {
//		log.Fatal(err)
//	}
package graceful

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

// Production timeout defaults. Every one of them exists because something
// broke in production without it:
//
//   - ReadHeaderTimeout: the slowloris guard. Headers must arrive fast;
//     the body can take longer.
//   - ReadTimeout: bounds the entire request read, body included.
//   - WriteTimeout: bounds the entire response write, handler time included.
//     Keep it above your slowest legitimate handler.
//   - IdleTimeout: how long a keep-alive connection may sit idle.
//   - ShutdownTimeout: how long in-flight requests and hooks get during
//     shutdown before the process gives up and exits.
const (
	DefaultAddr              = ":8080"
	DefaultReadHeaderTimeout = 5 * time.Second
	DefaultReadTimeout       = 10 * time.Second
	DefaultWriteTimeout      = 30 * time.Second
	DefaultIdleTimeout       = 120 * time.Second
	DefaultShutdownTimeout   = 20 * time.Second
)

// Hook runs during graceful shutdown with the shutdown context, which is
// cancelled when ShutdownTimeout elapses. Use hooks to close database pools,
// flush telemetry, or release any other resource. A hook that returns an
// error is logged; it never aborts the shutdown of the remaining hooks.
type Hook func(ctx context.Context) error

// Server is an http.Server with production timeouts, signal handling, and
// graceful shutdown. The zero value is not usable; construct one with New.
type Server struct {
	srv             *http.Server
	shutdownTimeout time.Duration
	logger          *slog.Logger
	hooks           []Hook
	certFile        string
	keyFile         string
}

// Option configures a Server.
type Option func(*Server)

// WithAddr sets the listen address. Default DefaultAddr (":8080").
func WithAddr(addr string) Option {
	return func(s *Server) { s.srv.Addr = addr }
}

// WithReadHeaderTimeout sets the maximum time to read request headers.
// Default DefaultReadHeaderTimeout. This is your slowloris protection;
// keep it small.
func WithReadHeaderTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.ReadHeaderTimeout = d }
}

// WithReadTimeout sets the maximum time to read the full request, body
// included. Default DefaultReadTimeout.
func WithReadTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.ReadTimeout = d }
}

// WithWriteTimeout sets the maximum time to write the full response,
// handler execution included. Default DefaultWriteTimeout. Raise it if you
// serve legitimately slow endpoints (exports, SSE); lower it otherwise.
func WithWriteTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.WriteTimeout = d }
}

// WithIdleTimeout sets the maximum time a keep-alive connection may stay
// idle. Default DefaultIdleTimeout.
func WithIdleTimeout(d time.Duration) Option {
	return func(s *Server) { s.srv.IdleTimeout = d }
}

// WithShutdownTimeout sets how long graceful shutdown may take before the
// shutdown context is cancelled. Default DefaultShutdownTimeout. It must
// cover your slowest in-flight request plus every shutdown hook.
func WithShutdownTimeout(d time.Duration) Option {
	return func(s *Server) { s.shutdownTimeout = d }
}

// WithLogger sets the logger used for lifecycle events. Default is a text
// slog logger writing to stderr. A nil logger keeps the default.
func WithLogger(l *slog.Logger) Option {
	return func(s *Server) {
		if l != nil {
			s.logger = l
		}
	}
}

// WithOnShutdown appends hooks run, in order, during graceful shutdown after
// the HTTP listener has stopped accepting new connections. Each hook gets
// the shutdown context; hook errors are logged and never abort shutdown.
func WithOnShutdown(hooks ...Hook) Option {
	return func(s *Server) { s.hooks = append(s.hooks, hooks...) }
}

// WithTLS enables HTTPS. Both files must be set or neither; passing only
// one is a configuration error reported by Run.
func WithTLS(certFile, keyFile string) Option {
	return func(s *Server) {
		s.certFile = certFile
		s.keyFile = keyFile
	}
}

// New builds a Server around handler with production timeout defaults,
// overridden by opts.
func New(handler http.Handler, opts ...Option) *Server {
	s := &Server{
		srv: &http.Server{
			Addr:              DefaultAddr,
			Handler:           handler,
			ReadHeaderTimeout: DefaultReadHeaderTimeout,
			ReadTimeout:       DefaultReadTimeout,
			WriteTimeout:      DefaultWriteTimeout,
			IdleTimeout:       DefaultIdleTimeout,
		},
		shutdownTimeout: DefaultShutdownTimeout,
		logger:          slog.New(slog.NewTextHandler(os.Stderr, nil)),
	}
	for _, opt := range opts {
		opt(s)
	}
	return s
}

// HTTPServer exposes the underlying [http.Server] for advanced use
// (custom ConnState hooks, http2 configuration, etc.).
func (s *Server) HTTPServer() *http.Server { return s.srv }

// Run starts the server and blocks until ctx is cancelled or the process
// receives SIGINT/SIGTERM, then gracefully shuts down: the listener stops
// accepting new connections, in-flight requests are drained, and shutdown
// hooks run. It returns nil on clean shutdown.
//
// If the server fails to start (bad address, TLS misconfiguration), Run
// returns that error instead of blocking.
func (s *Server) Run(ctx context.Context) error {
	if (s.certFile == "") != (s.keyFile == "") {
		return errors.New("graceful: WithTLS requires both cert and key files")
	}

	ctx, stop := signal.NotifyContext(ctx, syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	errCh := make(chan error, 1)
	go func() { errCh <- s.serve() }()

	select {
	case <-ctx.Done():
		s.logger.Info("shutdown initiated", "reason", ctx.Err())
		shutCtx, cancel := context.WithTimeout(context.Background(), s.shutdownTimeout)
		defer cancel()
		return errors.Join(s.graceful(shutCtx), <-errCh)
	case err := <-errCh:
		return err
	}
}

// Shutdown performs a graceful shutdown outside of Run: it stops accepting
// new connections, drains in-flight requests, and runs shutdown hooks. The
// context bounds the whole operation.
func (s *Server) Shutdown(ctx context.Context) error {
	return s.graceful(ctx)
}

// serve runs the listener; http.ErrServerClosed is a clean exit.
func (s *Server) serve() error {
	s.logger.Info("listening", "addr", s.srv.Addr, "tls", s.certFile != "")
	var err error
	if s.certFile != "" {
		err = s.srv.ListenAndServeTLS(s.certFile, s.keyFile)
	} else {
		err = s.srv.ListenAndServe()
	}
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return fmt.Errorf("graceful: serve: %w", err)
}

// graceful stops the listener, drains requests, and runs hooks in order.
func (s *Server) graceful(ctx context.Context) error {
	start := time.Now()

	if err := s.srv.Shutdown(ctx); err != nil {
		s.logger.Error("http shutdown failed", "error", err)
		return fmt.Errorf("graceful: http shutdown: %w", err)
	}

	for i, hook := range s.hooks {
		if err := hook(ctx); err != nil {
			// Logged, never fatal: one misbehaving hook must not
			// prevent the rest of the cleanup from running.
			s.logger.Error("shutdown hook failed", "hook", i, "error", err)
		}
	}

	s.logger.Info("shutdown complete", "duration", time.Since(start).Round(time.Millisecond))
	return nil
}
