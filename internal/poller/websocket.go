package poller

import (
	"context"
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relayra/relayra/internal/config"
	"github.com/relayra/relayra/internal/crypto"
	"github.com/relayra/relayra/internal/logger"
	"github.com/relayra/relayra/internal/models"
	"github.com/relayra/relayra/internal/proxy"
	"github.com/relayra/relayra/internal/store"
)

type webSocketFailureKind string

const (
	webSocketFailureNone       webSocketFailureKind = ""
	webSocketFailureConnection webSocketFailureKind = "connection"
	webSocketFailureInternal   webSocketFailureKind = "internal"
	webSocketFailureSelection  webSocketFailureKind = "selection"
)

type webSocketSessionResult struct {
	stable      bool
	shutdown    bool
	err         error
	proxyURL    string
	failureKind webSocketFailureKind
}

func runWebSocketMode(ctx context.Context, cfg *config.Config, rdb store.Backend,
	listenerInfo *models.Peer, proxyMgr *proxy.Manager, dispatcher *dispatcher,
	sigCh <-chan os.Signal) error {

	var cycle int64
	wsBackoff := cfg.WSReconnectBaseDuration()

	for {
		result := runWebSocketSession(ctx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, &cycle, sigCh)
		if result.shutdown || result.err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		classification := classifyRuntimeWebSocketError(result.err)

		if !cfg.WSEnableLongPollFallback && result.stable && result.failureKind == webSocketFailureConnection {
			slog.WarnContext(ctx, "websocket transport dropped, reconnecting immediately",
				"error", result.err,
				"classification", classification,
				"proxy", result.proxyURL,
			)
			wsBackoff = cfg.WSReconnectBaseDuration()
			continue
		}

		if result.failureKind == webSocketFailureConnection && result.proxyURL != "" && !result.stable {
			proxyMgr.MarkFailed(ctx, result.proxyURL)
		}

		if cfg.WSEnableLongPollFallback {
			slog.WarnContext(ctx, "websocket transport failed, falling back to long-poll",
				"error", result.err,
				"classification", classification,
				"retry_in", wsBackoff,
			)

			cycle++
			pollCtx := logger.WithPollCycle(ctx, cycle)
			_ = doPollCycleHTTP(pollCtx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, true)

			if result.stable {
				wsBackoff = cfg.WSReconnectBaseDuration()
			}
			retryDeadline := time.Now().Add(wsBackoff)
			nextBackoff := nextWebSocketBackoff(wsBackoff, cfg.WSReconnectMaxDuration())

			for time.Now().Before(retryDeadline) {
				cycle++
				pollCtx := logger.WithPollCycle(ctx, cycle)
				_ = doPollCycleHTTP(pollCtx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, true)
				if !sleepWithCancel(ctx, sigCh, activeWorkPollInterval) {
					return nil
				}
			}
			wsBackoff = nextBackoff
			continue
		}

		retryDelay := wsBackoff
		if result.stable {
			retryDelay = cfg.WSReconnectBaseDuration()
			wsBackoff = cfg.WSReconnectBaseDuration()
		} else {
			wsBackoff = nextWebSocketBackoff(wsBackoff, cfg.WSReconnectMaxDuration())
		}

		slog.WarnContext(ctx, "websocket transport failed, reconnecting websocket",
			"error", result.err,
			"classification", classification,
			"retry_in", retryDelay,
		)
		if !sleepWithCancel(ctx, sigCh, retryDelay) {
			return nil
		}
	}
}

func runWebSocketSession(ctx context.Context, cfg *config.Config, rdb store.Backend,
	listenerInfo *models.Peer, proxyMgr *proxy.Manager, dispatcher *dispatcher,
	cycle *int64, sigCh <-chan os.Signal) webSocketSessionResult {

	result := webSocketSessionResult{}
	_, proxyURL, err := proxyMgr.GetTransport(ctx)
	if err != nil {
		result.err = err
		result.failureKind = webSocketFailureSelection
		return result
	}
	result.proxyURL = proxyURL

	dialer, err := proxy.WebSocketDialerForProxy(proxyURL, time.Duration(cfg.RequestTimeout+15)*time.Second)
	if err != nil {
		result.err = err
		result.failureKind = webSocketFailureConnection
		return result
	}

	wsURL := fmt.Sprintf("ws://%s/api/v1/ws?peer_id=%s", listenerInfo.Address, url.QueryEscape(listenerInfo.ID))
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		result.err = err
		result.failureKind = webSocketFailureConnection
		return result
	}
	defer conn.Close()

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	slog.InfoContext(ctx, "websocket transport established", "proxy", proxyURL, "listener", listenerInfo.Address)

	_ = setWebSocketReadDeadline(conn, cfg)
	conn.SetPongHandler(func(string) error {
		return setWebSocketReadDeadline(conn, cfg)
	})

	pingTicker := time.NewTicker(cfg.WSPingIntervalDuration())
	defer pingTicker.Stop()

	go func() {
		for {
			select {
			case <-pingTicker.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte("relayra"), time.Now().Add(cfg.WSWriteTimeoutDuration()))
			case <-sessionCtx.Done():
				return
			}
		}
	}()

	probeSequence := 0
	for {
		select {
		case sig := <-sigCh:
			slog.InfoContext(ctx, "shutdown signal received", "signal", sig)
			_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "shutdown")
			result.shutdown = true
			return result
		case <-ctx.Done():
			_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "context cancelled")
			result.shutdown = true
			return result
		default:
		}

		*cycle = *cycle + 1
		pollCtx := logger.WithPollCycle(ctx, *cycle)

		payloadUp, leasedResults, err := buildPayloadUp(pollCtx, cfg, rdb)
		if err != nil {
			result.err = err
			result.failureKind = webSocketFailureInternal
			return result
		}
		waitSeconds := requestedWebSocketWait(cfg, dispatcher)
		expectedProbeID, expectedProbeSeq := attachWebSocketKeepaliveProbe(payloadUp, &probeSequence)

		ciphertext, nonce, timestamp, err := crypto.EncryptJSON(listenerInfo.EncryptionKey, payloadUp)
		if err != nil {
			result.err = err
			result.failureKind = webSocketFailureInternal
			return result
		}

		req := models.PollRequest{
			PeerID:      listenerInfo.ID,
			Nonce:       nonce,
			Timestamp:   timestamp,
			Payload:     ciphertext,
			WaitSeconds: waitSeconds,
		}

		_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeoutDuration()))
		if err := conn.WriteJSON(req); err != nil {
			result.err = err
			result.failureKind = webSocketFailureConnection
			return result
		}

		var resp models.PollResponse
		if err := conn.ReadJSON(&resp); err != nil {
			result.err = err
			result.failureKind = webSocketFailureConnection
			return result
		}
		_ = setWebSocketReadDeadline(conn, cfg)

		var payloadDown models.PollPayloadDown
		if err := crypto.DecryptJSON(listenerInfo.EncryptionKey, resp.Payload, resp.Nonce, resp.Timestamp, &payloadDown); err != nil {
			result.err = err
			result.failureKind = webSocketFailureInternal
			return result
		}
		if err := verifyWebSocketKeepaliveAck(payloadDown.Probe, expectedProbeID, expectedProbeSeq); err != nil {
			result.err = err
			result.failureKind = webSocketFailureInternal
			return result
		}
		if err := processPollResponse(pollCtx, cfg, rdb, dispatcher, &payloadDown); err != nil {
			result.err = err
			result.failureKind = webSocketFailureInternal
			return result
		}

		proxyMgr.MarkSuccess(pollCtx, proxyURL)

		slog.InfoContext(pollCtx, "websocket sync completed",
			"new_requests", len(payloadDown.Requests),
			"chunked_requests", len(payloadDown.RequestChunks),
			"acked_results", len(payloadDown.AckResultIDs),
			"results_sent", len(leasedResults),
			"known_request_states", len(payloadUp.RequestStates),
			"chunk_receipts", len(payloadUp.ChunkReceipts),
			"wait_seconds", waitSeconds,
		)
		result.stable = true

		if dispatcher.InFlight() > 0 {
			if !sleepWithCancel(ctx, sigCh, activeWorkPollInterval) {
				_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "dispatcher idle exit")
				result.shutdown = true
				return result
			}
		}
	}
}

func attachWebSocketKeepaliveProbe(payloadUp *models.PollPayloadUp, sequence *int) (string, int) {
	if payloadUp == nil || sequence == nil {
		return "", 0
	}
	*sequence = *sequence + 1
	probeID := fmt.Sprintf("runtime-%d-%d", time.Now().UnixNano(), *sequence)
	payloadUp.Probe = &models.ProbeMessage{
		ID:       probeID,
		Sequence: *sequence,
		SentAt:   time.Now().UnixMilli(),
	}
	return probeID, *sequence
}

func verifyWebSocketKeepaliveAck(probe *models.ProbeMessage, expectedID string, expectedSeq int) error {
	if expectedID == "" {
		return nil
	}
	if probe == nil {
		return fmt.Errorf("websocket keepalive acknowledgement missing")
	}
	if !probe.Ack || probe.ID != expectedID || probe.Sequence != expectedSeq {
		return fmt.Errorf("websocket keepalive acknowledgement mismatch")
	}
	return nil
}

func requestedWebSocketWait(cfg *config.Config, dispatcher *dispatcher) int {
	if dispatcher.InFlight() > 0 {
		return 0
	}
	waitSeconds := cfg.LongPollWait
	if cfg.WSKeepaliveInterval > 0 && (waitSeconds == 0 || cfg.WSKeepaliveInterval < waitSeconds) {
		waitSeconds = cfg.WSKeepaliveInterval
	}
	if waitSeconds < 0 {
		return 0
	}
	return waitSeconds
}

func nextWebSocketBackoff(current, max time.Duration) time.Duration {
	if current < max {
		current *= 2
		if current > max {
			current = max
		}
	}
	return current
}

func setWebSocketReadDeadline(conn *websocket.Conn, cfg *config.Config) error {
	return conn.SetReadDeadline(time.Now().Add(cfg.WSIdleTimeoutDuration()))
}

func writeWebSocketClose(conn *websocket.Conn, cfg *config.Config, code int, reason string) error {
	return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(cfg.WSWriteTimeoutDuration()))
}

func classifyRuntimeWebSocketError(err error) string {
	if err == nil {
		return ""
	}
	if closeErr, ok := err.(*websocket.CloseError); ok {
		return fmt.Sprintf("close:%d:%s", closeErr.Code, closeErr.Text)
	}
	if netErr, ok := err.(interface{ Timeout() bool }); ok && netErr.Timeout() {
		return "idle-timeout"
	}
	return err.Error()
}
