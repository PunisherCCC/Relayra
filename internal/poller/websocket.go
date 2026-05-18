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

func runWebSocketMode(ctx context.Context, cfg *config.Config, rdb store.Backend,
	listenerInfo *models.Peer, proxyMgr *proxy.Manager, dispatcher *dispatcher,
	sigCh <-chan os.Signal) error {

	var cycle int64
	wsBackoff := cfg.WSReconnectBaseDuration()

	for {
		stable, err := runWebSocketSession(ctx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, &cycle, sigCh)
		if err == nil {
			return nil
		}
		if ctx.Err() != nil {
			return nil
		}

		slog.WarnContext(ctx, "websocket transport failed, falling back to long-poll",
			"error", err,
			"classification", classifyRuntimeWebSocketError(err),
			"retry_in", wsBackoff,
		)

		cycle++
		pollCtx := logger.WithPollCycle(ctx, cycle)
		_ = doPollCycleHTTP(pollCtx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, true)

		if stable {
			wsBackoff = cfg.WSReconnectBaseDuration()
		}
		retryDeadline := time.Now().Add(wsBackoff)
		if wsBackoff < cfg.WSReconnectMaxDuration() {
			wsBackoff *= 2
			if wsBackoff > cfg.WSReconnectMaxDuration() {
				wsBackoff = cfg.WSReconnectMaxDuration()
			}
		}

		for time.Now().Before(retryDeadline) {
			cycle++
			pollCtx := logger.WithPollCycle(ctx, cycle)
			_ = doPollCycleHTTP(pollCtx, cfg, rdb, listenerInfo, proxyMgr, dispatcher, true)
			if !sleepWithCancel(ctx, sigCh, activeWorkPollInterval) {
				return nil
			}
		}
	}
}

func runWebSocketSession(ctx context.Context, cfg *config.Config, rdb store.Backend,
	listenerInfo *models.Peer, proxyMgr *proxy.Manager, dispatcher *dispatcher,
	cycle *int64, sigCh <-chan os.Signal) (bool, error) {

	_, proxyURL, err := proxyMgr.GetTransport(ctx)
	if err != nil {
		return false, err
	}

	dialer, err := proxy.WebSocketDialerForProxy(proxyURL, time.Duration(cfg.RequestTimeout+15)*time.Second)
	if err != nil {
		proxyMgr.MarkFailed(ctx, proxyURL)
		return false, err
	}

	wsURL := fmt.Sprintf("ws://%s/api/v1/ws?peer_id=%s", listenerInfo.Address, url.QueryEscape(listenerInfo.ID))
	conn, _, err := dialer.DialContext(ctx, wsURL, nil)
	if err != nil {
		proxyMgr.MarkFailed(ctx, proxyURL)
		return false, err
	}
	defer conn.Close()

	sessionCtx, cancel := context.WithCancel(ctx)
	defer cancel()

	proxyMgr.MarkSuccess(ctx, proxyURL)
	slog.InfoContext(ctx, "websocket transport established", "proxy", proxyURL, "listener", listenerInfo.Address)

	setWebSocketReadDeadline(conn, cfg)
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

	stable := false
	for {
		select {
		case sig := <-sigCh:
			slog.InfoContext(ctx, "shutdown signal received", "signal", sig)
			_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "shutdown")
			return true, nil
		case <-ctx.Done():
			_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "context cancelled")
			return true, nil
		default:
		}

		*cycle = *cycle + 1
		pollCtx := logger.WithPollCycle(ctx, *cycle)

		payloadUp, leasedResults, err := buildPayloadUp(pollCtx, cfg, rdb)
		if err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}
		ciphertext, nonce, timestamp, err := crypto.EncryptJSON(listenerInfo.EncryptionKey, payloadUp)
		if err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}

		req := models.PollRequest{
			PeerID:      listenerInfo.ID,
			Nonce:       nonce,
			Timestamp:   timestamp,
			Payload:     ciphertext,
			WaitSeconds: requestedPollWait(cfg, dispatcher, true),
		}

		_ = conn.SetWriteDeadline(time.Now().Add(cfg.WSWriteTimeoutDuration()))
		if err := conn.WriteJSON(req); err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}

		var resp models.PollResponse
		if err := conn.ReadJSON(&resp); err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}

		var payloadDown models.PollPayloadDown
		if err := crypto.DecryptJSON(listenerInfo.EncryptionKey, resp.Payload, resp.Nonce, resp.Timestamp, &payloadDown); err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}
		if err := processPollResponse(pollCtx, cfg, rdb, dispatcher, &payloadDown); err != nil {
			proxyMgr.MarkFailed(pollCtx, proxyURL)
			return stable, err
		}

		slog.InfoContext(pollCtx, "websocket sync completed",
			"new_requests", len(payloadDown.Requests),
			"chunked_requests", len(payloadDown.RequestChunks),
			"acked_results", len(payloadDown.AckResultIDs),
			"results_sent", len(leasedResults),
			"known_request_states", len(payloadUp.RequestStates),
			"chunk_receipts", len(payloadUp.ChunkReceipts),
		)
		stable = true

		if dispatcher.InFlight() > 0 {
			if !sleepWithCancel(ctx, sigCh, activeWorkPollInterval) {
				_ = writeWebSocketClose(conn, cfg, websocket.CloseNormalClosure, "dispatcher idle exit")
				return true, nil
			}
		}
	}
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
