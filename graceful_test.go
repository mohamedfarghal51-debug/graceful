package graceful

import (
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"sync/atomic"
	"testing"
	"time"
)

// freeAddr returns a localhost address with a currently-free port.
// There is an inherent TOCTOU race; acceptable for tests.
func freeAddr(t *testing.T) string {
	t.Helper()
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer ln.Close()
	return ln.Addr().String()
}

func waitUp(t *testing.T, addr string) {
	t.Helper()
	deadline := time.Now().Add(3 * time.Second)
	for time.Now().Before(deadline) {
		conn, err := net.DialTimeout("tcp", addr, 100*time.Millisecond)
		if err == nil {
			conn.Close()
			return
		}
		time.Sleep(20 * time.Millisecond)
	}
	t.Fatal("server did not start in time")
}

func TestDefaults(t *testing.T) {
	s := New(http.NotFoundHandler())
	if s.srv.Addr != DefaultAddr {
		t.Errorf("Addr = %q, want %q", s.srv.Addr, DefaultAddr)
	}
	cases := []struct {
		name string
		got  time.Duration
		want time.Duration
	}{
		{"ReadHeaderTimeout", s.srv.ReadHeaderTimeout, DefaultReadHeaderTimeout},
		{"ReadTimeout", s.srv.ReadTimeout, DefaultReadTimeout},
		{"WriteTimeout", s.srv.WriteTimeout, DefaultWriteTimeout},
		{"IdleTimeout", s.srv.IdleTimeout, DefaultIdleTimeout},
		{"shutdownTimeout", s.shutdownTimeout, DefaultShutdownTimeout},
	}
	for _, c := range cases {
		if c.got != c.want {
			t.Errorf("%s = %v, want %v", c.name, c.got, c.want)
		}
	}
}

func TestOptions(t *testing.T) {
	s := New(http.NotFoundHandler(),
		WithAddr("127.0.0.1:9999"),
		WithReadHeaderTimeout(time.Second),
		WithReadTimeout(2*time.Second),
		WithWriteTimeout(3*time.Second),
		WithIdleTimeout(4*time.Second),
		WithShutdownTimeout(5*time.Second),
	)
	if s.srv.Addr != "127.0.0.1:9999" {
		t.Errorf("Addr = %q", s.srv.Addr)
	}
	if s.srv.ReadHeaderTimeout != time.Second ||
		s.srv.ReadTimeout != 2*time.Second ||
		s.srv.WriteTimeout != 3*time.Second ||
		s.srv.IdleTimeout != 4*time.Second ||
		s.shutdownTimeout != 5*time.Second {
		t.Error("options were not applied")
	}
}

func TestRun_ServesRequests(t *testing.T) {
	addr := freeAddr(t)
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Write([]byte("ok"))
	}), WithAddr(addr))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	waitUp(t, addr)

	resp, err := http.Get("http://" + addr)
	if err != nil {
		t.Fatal(err)
	}
	body, _ := io.ReadAll(resp.Body)
	resp.Body.Close()
	if string(body) != "ok" {
		t.Errorf("body = %q, want %q", body, "ok")
	}

	cancel()
	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after cancel")
	}
}

func TestRun_DrainsInflightRequests(t *testing.T) {
	addr := freeAddr(t)
	s := New(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		time.Sleep(300 * time.Millisecond) // simulate real work
		w.Write([]byte("done"))
	}), WithAddr(addr), WithShutdownTimeout(5*time.Second))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()

	waitUp(t, addr)

	// Fire a request, then cancel while it is in flight.
	respCh := make(chan string, 1)
	go func() {
		resp, err := http.Get("http://" + addr)
		if err != nil {
			respCh <- "error: " + err.Error()
			return
		}
		defer resp.Body.Close()
		body, _ := io.ReadAll(resp.Body)
		respCh <- string(body)
	}()
	time.Sleep(100 * time.Millisecond) // ensure the handler is sleeping
	cancel()

	select {
	case body := <-respCh:
		if body != "done" {
			t.Errorf("in-flight request got %q, want %q (was killed mid-flight)", body, "done")
		}
	case <-time.After(5 * time.Second):
		t.Fatal("in-flight request never completed")
	}

	select {
	case err := <-runErr:
		if err != nil {
			t.Errorf("Run returned %v, want nil", err)
		}
	case <-time.After(5 * time.Second):
		t.Fatal("Run did not return after drain")
	}
}

func TestRun_HooksRunInOrder(t *testing.T) {
	addr := freeAddr(t)
	var order []int
	var hookCtxHasDeadline atomic.Bool
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error {
			_, hasDeadline := ctx.Deadline()
			hookCtxHasDeadline.Store(hasDeadline)
			order = append(order, 1)
			return nil
		},
		func(ctx context.Context) error {
			order = append(order, 2)
			return nil
		},
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)
	cancel()

	if err := <-runErr; err != nil {
		t.Fatalf("Run returned %v", err)
	}
	if len(order) != 2 || order[0] != 1 || order[1] != 2 {
		t.Errorf("hooks ran in order %v, want [1 2]", order)
	}
	if !hookCtxHasDeadline.Load() {
		t.Error("hook context had no deadline")
	}
}

func TestRun_HookErrorDoesNotFailShutdown(t *testing.T) {
	addr := freeAddr(t)
	var secondRan atomic.Bool
	s := New(http.NotFoundHandler(), WithAddr(addr), WithOnShutdown(
		func(ctx context.Context) error { return errors.New("test hook error") },
		func(ctx context.Context) error { secondRan.Store(true); return nil },
	))

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	runErr := make(chan error, 1)
	go func() { runErr <- s.Run(ctx) }()
	waitUp(t, addr)
	cancel()

	if err := <-runErr; err != nil {
		t.Errorf("Run returned %v, want nil despite hook error", err)
	}
	if !secondRan.Load() {
		t.Error("second hook did not run after first hook failed")
	}
}

func TestRun_TLSConfigError(t *testing.T) {
	s := New(http.NotFoundHandler(), WithTLS("cert.pem", ""))
	if err := s.Run(context.Background()); err == nil {
		t.Error("expected error for cert-without-key, got nil")
	}
}
