package poller

import (
	"context"
	"path/filepath"
	"testing"
	"time"

	"github.com/gorilla/websocket"
	"github.com/relayra/relayra/internal/models"
	"github.com/relayra/relayra/internal/store"
	"github.com/relayra/relayra/internal/transport"
)

func TestNextWebSocketBackoffCapsAtMax(t *testing.T) {
	got := nextWebSocketBackoff(20*time.Second, 30*time.Second)
	if got != 30*time.Second {
		t.Fatalf("nextWebSocketBackoff() = %s, want 30s", got)
	}
}

func TestClassifyWebSocketReadFailureKind(t *testing.T) {
	tests := []struct {
		name string
		err  error
		want webSocketFailureKind
	}{
		{
			name: "server internal close is internal",
			err:  &websocket.CloseError{Code: websocket.CloseInternalServerErr, Text: "poll handling failed"},
			want: webSocketFailureInternal,
		},
		{
			name: "policy close is internal",
			err:  &websocket.CloseError{Code: websocket.ClosePolicyViolation, Text: "peer mismatch"},
			want: webSocketFailureInternal,
		},
		{
			name: "abnormal close is connection",
			err:  &websocket.CloseError{Code: websocket.CloseAbnormalClosure, Text: "unexpected EOF"},
			want: webSocketFailureConnection,
		},
	}

	for _, tt := range tests {
		if got := classifyWebSocketReadFailureKind(tt.err); got != tt.want {
			t.Fatalf("%s: classifyWebSocketReadFailureKind() = %q, want %q", tt.name, got, tt.want)
		}
	}
}

func TestHandleSenderOutboxAckCleansUpState(t *testing.T) {
	ctx := context.Background()
	dbPath := filepath.Join(t.TempDir(), "relayra.db")
	rdb, err := store.NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite() error = %v", err)
	}
	defer rdb.Close()

	result := &models.RelayResult{
		RequestID:  "req-1",
		StatusCode: 200,
		Body:       "ok",
		ExecutedAt: time.Now(),
	}
	if err := rdb.PushResult(ctx, result); err != nil {
		t.Fatalf("PushResult() error = %v", err)
	}
	if err := rdb.StoreSenderRequestState(ctx, &models.RequestSyncState{
		RequestID:  result.RequestID,
		Status:     models.StatusCompleted,
		LeaseUntil: time.Now().Add(time.Minute),
		UpdatedAt:  time.Now(),
	}); err != nil {
		t.Fatalf("StoreSenderRequestState() error = %v", err)
	}

	receipt, _, err := rdb.StoreInboundChunk(ctx, models.TransportChunk{
		TransferID: "transfer-1",
		RequestID:  "chunk-req",
		Kind:       models.TransportChunkKindRequest,
		Index:      0,
		Total:      1,
		Payload:    "e30=",
		Checksum:   models.SHA256Hex([]byte("{}")),
		TotalSize:  2,
	}, time.Minute)
	if err != nil {
		t.Fatalf("StoreInboundChunk() error = %v", err)
	}
	if receipt == nil {
		t.Fatalf("StoreInboundChunk() receipt = nil, want receipt")
	}

	scope := models.SenderWSScope("listener-1")
	if _, err := transport.EnqueueWSMessage(ctx, rdb, scope, &models.WSMessage{
		Type:   models.WSMessageTypeResult,
		PeerID: "listener-1",
		Result: result,
	}, result.RequestID); err != nil {
		t.Fatalf("EnqueueWSMessage(result) error = %v", err)
	}
	if _, err := transport.EnqueueWSMessage(ctx, rdb, scope, &models.WSMessage{
		Type:         models.WSMessageTypeChunkReceipt,
		PeerID:       "listener-1",
		ChunkReceipt: receipt,
	}, receipt.TransferID); err != nil {
		t.Fatalf("EnqueueWSMessage(chunk_receipt) error = %v", err)
	}

	if err := handleSenderOutboxAck(ctx, rdb, scope, 2); err != nil {
		t.Fatalf("handleSenderOutboxAck() error = %v", err)
	}

	if count, err := rdb.PendingResultsCount(ctx); err != nil {
		t.Fatalf("PendingResultsCount() error = %v", err)
	} else if count != 0 {
		t.Fatalf("PendingResultsCount() = %d, want 0", count)
	}
	if state, err := rdb.GetSenderRequestState(ctx, result.RequestID); err != nil {
		t.Fatalf("GetSenderRequestState() error = %v", err)
	} else if state != nil {
		t.Fatalf("GetSenderRequestState() = %+v, want nil after ack", state)
	}
	if receipts, err := rdb.ListChunkReceipts(ctx); err != nil {
		t.Fatalf("ListChunkReceipts() error = %v", err)
	} else if len(receipts) != 0 {
		t.Fatalf("ListChunkReceipts() len = %d, want 0 after ack", len(receipts))
	}
}
