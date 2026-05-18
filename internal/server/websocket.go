package server

import (
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"net/http"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relayra/relayra/internal/config"
	"github.com/relayra/relayra/internal/logger"
	"github.com/relayra/relayra/internal/models"
)

var wsUpgrader = websocket.Upgrader{
	CheckOrigin: func(r *http.Request) bool { return true },
}

// WebSocket handles persistent sender connections using the same encrypted
// request/response envelopes as the HTTP poll endpoint.
func (h *Handlers) WebSocket(w http.ResponseWriter, r *http.Request) {
	ctx := logger.WithComponent(r.Context(), "server")
	peerID := r.URL.Query().Get("peer_id")
	if peerID == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "peer_id is required"})
		return
	}

	conn, err := wsUpgrader.Upgrade(w, r, nil)
	if err != nil {
		slog.ErrorContext(ctx, "websocket upgrade failed", "error", err)
		return
	}
	defer conn.Close()

	baseCtx, cancel := context.WithCancel(context.Background())
	defer cancel()
	baseCtx = logger.WithComponent(baseCtx, "server")
	baseCtx = logger.WithPeerID(baseCtx, peerID)

	pingTicker := time.NewTicker(h.cfg.WSPingIntervalDuration())
	defer pingTicker.Stop()

	_ = conn.SetReadDeadline(time.Now().Add(h.cfg.WSIdleTimeoutDuration()))
	conn.SetPongHandler(func(string) error {
		return conn.SetReadDeadline(time.Now().Add(h.cfg.WSIdleTimeoutDuration()))
	})

	go func() {
		for {
			select {
			case <-pingTicker.C:
				_ = conn.WriteControl(websocket.PingMessage, []byte("relayra"), time.Now().Add(h.cfg.WSWriteTimeoutDuration()))
			case <-baseCtx.Done():
				return
			}
		}
	}()

	for {
		_, message, err := conn.ReadMessage()
		if err != nil {
			slog.WarnContext(baseCtx, "websocket read failed", "error", err, "classification", classifyServerWebSocketError(err))
			_ = writeServerWebSocketClose(conn, h.cfg, websocket.CloseGoingAway, "read failure")
			return
		}
		_ = conn.SetReadDeadline(time.Now().Add(h.cfg.WSIdleTimeoutDuration()))

		var pollReq models.PollRequest
		if err := json.Unmarshal(message, &pollReq); err != nil {
			slog.WarnContext(baseCtx, "invalid websocket poll message", "error", err)
			_ = writeServerWebSocketClose(conn, h.cfg, websocket.CloseUnsupportedData, "invalid poll message")
			return
		}
		if pollReq.PeerID == "" {
			pollReq.PeerID = peerID
		}
		if pollReq.PeerID != peerID {
			slog.WarnContext(baseCtx, "websocket peer mismatch", "peer_id", pollReq.PeerID)
			_ = writeServerWebSocketClose(conn, h.cfg, websocket.ClosePolicyViolation, "peer mismatch")
			return
		}

		resp, err := h.handlePollMessage(baseCtx, peerID, pollReq.Payload, pollReq.Nonce, pollReq.Timestamp, pollReq.WaitSeconds)
		if err != nil {
			slog.WarnContext(baseCtx, "failed to handle websocket poll", "error", err)
			_ = writeServerWebSocketClose(conn, h.cfg, websocket.CloseInternalServerErr, "poll handling failed")
			return
		}

		_ = conn.SetWriteDeadline(time.Now().Add(h.cfg.WSWriteTimeoutDuration()))
		if err := conn.WriteJSON(resp); err != nil {
			slog.WarnContext(baseCtx, "websocket write failed", "error", err, "classification", classifyServerWebSocketError(err))
			_ = writeServerWebSocketClose(conn, h.cfg, websocket.CloseAbnormalClosure, "write failure")
			return
		}
	}
}

func writeServerWebSocketClose(conn *websocket.Conn, cfg *config.Config, code int, reason string) error {
	return conn.WriteControl(websocket.CloseMessage, websocket.FormatCloseMessage(code, reason), time.Now().Add(cfg.WSWriteTimeoutDuration()))
}

func classifyServerWebSocketError(err error) string {
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
