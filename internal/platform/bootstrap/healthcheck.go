package bootstrap

import (
	"context"
	"net"
	"net/http"
	"os"
	"time"
)

// HandleHealthcheckCommand makes `<binary> healthcheck` probe the local /readyz and exit 0 or 1.
// Distroless images have no shell, curl or wget, so the binary checks itself. Use it for the
// Docker HEALTHCHECK and compose healthchecks (Kubernetes uses httpGet probes instead).
// Call it first thing in main.
func HandleHealthcheckCommand() {
	if len(os.Args) < 2 || os.Args[1] != "healthcheck" {
		return
	}
	os.Exit(probeReadyz())
}

// probeReadyz returns the process exit code. It is separate from the os.Exit call so deferred
// cleanup runs.
func probeReadyz() int {
	addr := os.Getenv("OPS_ADDR")
	if addr == "" {
		addr = ":9090"
	}
	_, port, err := net.SplitHostPort(addr)
	if err != nil {
		return 1
	}
	ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
	defer cancel()

	// The host is the fixed loopback address and the port comes from the operator's own config,
	// so there is no attacker controlled destination.
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, "http://127.0.0.1:"+port+"/readyz", http.NoBody) //nolint:gosec // G704: fixed loopback host
	if err != nil {
		return 1
	}
	resp, err := http.DefaultClient.Do(req) //nolint:gosec // G704: fixed loopback host
	if err != nil {
		return 1
	}
	defer func() { _ = resp.Body.Close() }()
	if resp.StatusCode != http.StatusOK {
		return 1
	}
	return 0
}
