package proxy

import (
	"context"
	"fmt"
	"net"
	"net/http"
	"net/url"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relayra/relayra/internal/config"
	"github.com/relayra/relayra/internal/crypto"
	"github.com/relayra/relayra/internal/models"
	xproxy "golang.org/x/net/proxy"
)

// WebSocketSampleResult holds metrics for one websocket reliability sample.
type WebSocketSampleResult struct {
	Sample           int
	AliveDuration    time.Duration
	ProbesSent       int
	ProbesAcked      int
	HandshakeOK      bool
	DisconnectReason string
	Score            int
}

// WebSocketReliabilityResult summarizes a websocket reliability run for one proxy.
type WebSocketReliabilityResult struct {
	ProxyURL string
	Samples  []WebSocketSampleResult
	Score    int
	Grade    string
}

// WebSocketDialerForProxy returns a websocket dialer configured for the given proxy URL.
func WebSocketDialerForProxy(proxyURL string, handshakeTimeout time.Duration) (*websocket.Dialer, error) {
	dialer := websocket.Dialer{HandshakeTimeout: handshakeTimeout}
	if proxyURL == "" || proxyURL == "direct" {
		return &dialer, nil
	}

	parsed, err := url.Parse(proxyURL)
	if err != nil {
		return nil, err
	}

	switch parsed.Scheme {
	case "http", "https":
		dialer.Proxy = http.ProxyURL(parsed)
		dialer.NetDialContext = (&net.Dialer{
			Timeout:   10 * time.Second,
			KeepAlive: 30 * time.Second,
		}).DialContext
	case "socks5", "socks5h":
		var auth *xproxy.Auth
		if parsed.User != nil {
			auth = &xproxy.Auth{User: parsed.User.Username()}
			auth.Password, _ = parsed.User.Password()
		}
		base, err := xproxy.SOCKS5("tcp", parsed.Host, auth, xproxy.Direct)
		if err != nil {
			return nil, err
		}
		ctxDialer, ok := base.(xproxy.ContextDialer)
		if !ok {
			return nil, fmt.Errorf("SOCKS5 dialer does not support context")
		}
		dialer.NetDialContext = ctxDialer.DialContext
	default:
		return nil, fmt.Errorf("unsupported proxy scheme: %s", parsed.Scheme)
	}

	return &dialer, nil
}

// TestWebSocketReliability exercises websocket delivery against the paired listener via a proxy.
func TestWebSocketReliability(ctx context.Context, cfg *config.Config, listener *models.Peer, proxyURL string, samples, holdSeconds, intervalSeconds int) (*WebSocketReliabilityResult, error) {
	if listener == nil {
		return nil, fmt.Errorf("paired listener info is required")
	}
	if samples < 1 {
		samples = 3
	}
	if holdSeconds < 1 {
		holdSeconds = 30
	}
	if intervalSeconds < 1 {
		intervalSeconds = 5
	}

	result := &WebSocketReliabilityResult{
		ProxyURL: proxyURL,
		Samples:  make([]WebSocketSampleResult, 0, samples),
	}

	for i := 1; i <= samples; i++ {
		sample := runWebSocketReliabilitySample(ctx, cfg, listener, proxyURL, i, holdSeconds, intervalSeconds)
		result.Samples = append(result.Samples, sample)
		result.Score += sample.Score
	}
	result.Score = result.Score / len(result.Samples)
	result.Grade = gradeReliability(result.Score)
	return result, nil
}

func runWebSocketReliabilitySample(ctx context.Context, cfg *config.Config, listener *models.Peer, proxyURL string, sampleNo, holdSeconds, intervalSeconds int) WebSocketSampleResult {
	holdDuration := time.Duration(holdSeconds) * time.Second
	interval := time.Duration(intervalSeconds) * time.Second
	sample := WebSocketSampleResult{Sample: sampleNo}

	dialer, err := WebSocketDialerForProxy(proxyURL, time.Duration(cfg.RequestTimeout+15)*time.Second)
	if err != nil {
		sample.DisconnectReason = err.Error()
		return sample
	}

	wsURL := fmt.Sprintf("ws://%s/api/v1/ws?peer_id=%s", listener.Address, url.QueryEscape(listener.ID))
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		sample.DisconnectReason = classifyWebSocketError(err)
		return sample
	}
	defer conn.Close()

	sample.HandshakeOK = true
	_ = conn.SetReadDeadline(time.Now().Add(cfg.WSIdleTimeoutDuration()))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(cfg.WSIdleTimeoutDuration()))
	})

	pingTicker := time.NewTicker(cfg.WSPingIntervalDuration())
	defer pingTicker.Stop()
	stopPing := make(chan struct{})
	go func() {
		for {
			select {
			case <-pingTicker.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte("relayra"), time.Now().Add(cfg.WSWriteTimeoutDuration()))
			case <-stopPing:
				return
			case <-ctx.Done():
				return
			}
		}
	}()
	defer close(stopPing)

	start := time.Now()
	deadline := start.Add(holdDuration)
	sequence := 1
loop:
	for {
		if time.Now().After(deadline) {
			break
		}
		probeID := fmt.Sprintf("%d-%d", time.Now().UnixNano(), sequence)
		payloadUp := &models.PollPayloadUp{
			Probe: &models.ProbeMessage{
				ID:        probeID,
				Sequence:  sequence,
				SentAt:    time.Now().UnixMilli(),
				ProbeOnly: true,
			},
		}
		ciphertext, nonce, timestamp, err := crypto.EncryptJSON(listener.EncryptionKey, payloadUp)
		if err != nil {
			sample.DisconnectReason = err.Error()
			break
		}

		req := models.PollRequest{
			PeerID:      listener.ID,
			Nonce:       nonce,
			Timestamp:   timestamp,
			Payload:     ciphertext,
			WaitSeconds: 0,
		}

		sample.ProbesSent++
		_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeoutDuration()))
		if err := conn.WriteJSON(req); err != nil {
			sample.DisconnectReason = classifyWebSocketError(err)
			break
		}

		var resp models.PollResponse
		if err := conn.ReadJSON(&resp); err != nil {
			sample.DisconnectReason = classifyWebSocketError(err)
			break
		}

		var payloadDown models.PollPayloadDown
		if err := crypto.DecryptJSON(listener.EncryptionKey, resp.Payload, resp.Nonce, resp.Timestamp, &payloadDown); err != nil {
			sample.DisconnectReason = "probe decrypt failure"
			break
		}
		if payloadDown.Probe == nil || !payloadDown.Probe.Ack || payloadDown.Probe.ID != probeID || payloadDown.Probe.Sequence != sequence {
			sample.DisconnectReason = "probe acknowledgement mismatch"
			break
		}

		sample.ProbesAcked++
		sequence++

		sleepFor := interval
		if remaining := time.Until(deadline); remaining < sleepFor {
			sleepFor = remaining
		}
		if sleepFor <= 0 {
			break
		}
		select {
		case <-time.After(sleepFor):
		case <-ctx.Done():
			sample.DisconnectReason = "cancelled"
			break loop
		}
	}

	sample.AliveDuration = time.Since(start)
	if sample.AliveDuration > holdDuration {
		sample.AliveDuration = holdDuration
	}
	sample.Score = scoreReliabilitySample(sample, holdDuration)
	if sample.DisconnectReason == "" {
		_ = conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(websocket.CloseNormalClosure, "reliability sample complete"), time.Now().Add(cfg.WSWriteTimeoutDuration()))
	}
	return sample
}

func scoreReliabilitySample(sample WebSocketSampleResult, holdDuration time.Duration) int {
	uptimeComponent := 0.0
	if holdDuration > 0 {
		uptimeComponent = float64(sample.AliveDuration) / float64(holdDuration)
	}
	if uptimeComponent > 1 {
		uptimeComponent = 1
	}

	deliveryComponent := 1.0
	if sample.ProbesSent > 0 {
		deliveryComponent = float64(sample.ProbesAcked) / float64(sample.ProbesSent)
	}
	score := int((uptimeComponent * deliveryComponent * 100) + 0.5)
	if !sample.HandshakeOK {
		return 0
	}
	if score < 0 {
		return 0
	}
	if score > 100 {
		return 100
	}
	return score
}

func gradeReliability(score int) string {
	switch {
	case score >= 90:
		return "excellent"
	case score >= 70:
		return "usable"
	case score >= 40:
		return "fragile"
	default:
		return "poor"
	}
}

func classifyWebSocketError(err error) string {
	if err == nil {
		return ""
	}
	if closeErr, ok := err.(*websocket.CloseError); ok {
		return fmt.Sprintf("websocket close %d: %s", closeErr.Code, closeErr.Text)
	}
	return err.Error()
}
