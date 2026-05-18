package poller

import (
	"testing"
	"time"

	"github.com/relayra/relayra/internal/config"
)

func TestRequestedWebSocketWaitUsesKeepaliveWindow(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LongPollWait = 30
	cfg.WSKeepaliveInterval = 5

	d := &dispatcher{}
	if got := requestedWebSocketWait(cfg, d); got != 5 {
		t.Fatalf("requestedWebSocketWait() = %d, want 5", got)
	}
}

func TestRequestedWebSocketWaitIsZeroWithInFlightWork(t *testing.T) {
	cfg := config.DefaultConfig()
	cfg.LongPollWait = 30
	cfg.WSKeepaliveInterval = 5

	d := &dispatcher{}
	d.inFlight.Store(1)
	if got := requestedWebSocketWait(cfg, d); got != 0 {
		t.Fatalf("requestedWebSocketWait() = %d, want 0", got)
	}
}

func TestNextWebSocketBackoffCapsAtMax(t *testing.T) {
	got := nextWebSocketBackoff(20*time.Second, 30*time.Second)
	if got != 30*time.Second {
		t.Fatalf("nextWebSocketBackoff() = %s, want 30s", got)
	}
}

func TestVerifyWebSocketKeepaliveAck(t *testing.T) {
	if err := verifyWebSocketKeepaliveAck(nil, "", 0); err != nil {
		t.Fatalf("verifyWebSocketKeepaliveAck() unexpected error for empty expectation: %v", err)
	}
}
