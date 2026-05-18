package store

import (
	"context"
	"encoding/base64"
	"encoding/json"
	"path/filepath"
	"testing"
	"time"

	"github.com/relayra/relayra/internal/models"
)

func newTestSQLite(t *testing.T) *SQLite {
	t.Helper()

	dbPath := filepath.Join(t.TempDir(), "relayra-test.db")
	s, err := NewSQLite(dbPath)
	if err != nil {
		t.Fatalf("NewSQLite() error = %v", err)
	}
	t.Cleanup(func() { _ = s.Close() })
	return s
}

func TestSQLiteLeaseRequestsRedeliversAfterExpiry(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	req := &models.RelayRequest{
		ID:         "req-1",
		WebhookURL: "http://example.test/hook",
		Request: models.HTTPRequest{
			URL:    "http://127.0.0.1:8080/health",
			Method: "GET",
		},
		Status:    models.StatusQueued,
		CreatedAt: time.Now(),
	}
	if err := s.EnqueueRequest(ctx, "peer-1", req); err != nil {
		t.Fatalf("EnqueueRequest() error = %v", err)
	}

	leased, err := s.LeaseRequests(ctx, "peer-1", 10, time.Second)
	if err != nil {
		t.Fatalf("LeaseRequests() error = %v", err)
	}
	if len(leased) != 1 || leased[0].ID != req.ID {
		t.Fatalf("LeaseRequests() = %+v, want one leased request %q", leased, req.ID)
	}

	leased, err = s.LeaseRequests(ctx, "peer-1", 10, time.Second)
	if err != nil {
		t.Fatalf("second LeaseRequests() error = %v", err)
	}
	if len(leased) != 0 {
		t.Fatalf("second LeaseRequests() len = %d, want 0 before lease expiry", len(leased))
	}

	time.Sleep(1100 * time.Millisecond)

	leased, err = s.LeaseRequests(ctx, "peer-1", 10, time.Second)
	if err != nil {
		t.Fatalf("third LeaseRequests() error = %v", err)
	}
	if len(leased) != 1 || leased[0].ID != req.ID {
		t.Fatalf("third LeaseRequests() = %+v, want redelivery of %q after expiry", leased, req.ID)
	}

	if err := s.StoreResult(ctx, &models.RelayResult{
		RequestID:  req.ID,
		StatusCode: 200,
		Body:       "ok",
		ExecutedAt: time.Now(),
	}, 60); err != nil {
		t.Fatalf("StoreResult() error = %v", err)
	}

	qLen, err := s.QueueLength(ctx, "peer-1")
	if err != nil {
		t.Fatalf("QueueLength() error = %v", err)
	}
	if qLen != 0 {
		t.Fatalf("QueueLength() = %d, want 0 after result storage", qLen)
	}
}

func TestSQLiteLeaseResultsUntilAck(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	result := &models.RelayResult{
		RequestID:  "req-2",
		StatusCode: 201,
		Body:       "created",
		ExecutedAt: time.Now(),
	}
	if err := s.PushResult(ctx, result); err != nil {
		t.Fatalf("PushResult() error = %v", err)
	}
	if err := s.StoreSenderRequestState(ctx, &models.RequestSyncState{
		RequestID:  result.RequestID,
		Status:     models.StatusCompleted,
		LeaseUntil: time.Now().Add(time.Minute),
		UpdatedAt:  time.Now(),
	}); err != nil {
		t.Fatalf("StoreSenderRequestState() error = %v", err)
	}

	leased, err := s.LeaseResults(ctx, 10, time.Second)
	if err != nil {
		t.Fatalf("LeaseResults() error = %v", err)
	}
	if len(leased) != 1 || leased[0].RequestID != result.RequestID {
		t.Fatalf("LeaseResults() = %+v, want one leased result %q", leased, result.RequestID)
	}

	leased, err = s.LeaseResults(ctx, 10, time.Second)
	if err != nil {
		t.Fatalf("second LeaseResults() error = %v", err)
	}
	if len(leased) != 0 {
		t.Fatalf("second LeaseResults() len = %d, want 0 before lease expiry", len(leased))
	}

	time.Sleep(1100 * time.Millisecond)

	leased, err = s.LeaseResults(ctx, 10, time.Second)
	if err != nil {
		t.Fatalf("third LeaseResults() error = %v", err)
	}
	if len(leased) != 1 || leased[0].RequestID != result.RequestID {
		t.Fatalf("third LeaseResults() = %+v, want redelivery of %q after expiry", leased, result.RequestID)
	}

	if err := s.AckResults(ctx, []string{result.RequestID}); err != nil {
		t.Fatalf("AckResults() error = %v", err)
	}

	count, err := s.PendingResultsCount(ctx)
	if err != nil {
		t.Fatalf("PendingResultsCount() error = %v", err)
	}
	if count != 0 {
		t.Fatalf("PendingResultsCount() = %d, want 0 after ack", count)
	}

	state, err := s.GetSenderRequestState(ctx, result.RequestID)
	if err != nil {
		t.Fatalf("GetSenderRequestState() error = %v", err)
	}
	if state != nil {
		t.Fatalf("GetSenderRequestState() = %+v, want nil after ack cleanup", state)
	}
}

func TestSQLiteStoreInboundChunkReassemblesRequest(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	req := models.RelayRequest{
		ID: "req-chunked",
		Request: models.HTTPRequest{
			URL:    "http://127.0.0.1:8080/large",
			Method: "POST",
			Body:   string(make([]byte, 300000)),
		},
		Status:    models.StatusQueued,
		CreatedAt: time.Now(),
	}

	payload, err := json.Marshal(req)
	if err != nil {
		t.Fatalf("json.Marshal() error = %v", err)
	}
	chunkSize := 131072
	total := (len(payload) + chunkSize - 1) / chunkSize
	checksum := models.SHA256Hex(payload)

	var reassembled *models.RelayRequest
	for i := 0; i < total; i++ {
		start := i * chunkSize
		end := start + chunkSize
		if end > len(payload) {
			end = len(payload)
		}
		receipt, assembled, err := s.StoreInboundChunk(ctx, models.TransportChunk{
			TransferID: models.RequestTransferID(req.ID),
			RequestID:  req.ID,
			Kind:       models.TransportChunkKindRequest,
			Index:      i,
			Total:      total,
			Payload:    base64.StdEncoding.EncodeToString(payload[start:end]),
			Checksum:   checksum,
			TotalSize:  len(payload),
		}, time.Minute)
		if err != nil {
			t.Fatalf("StoreInboundChunk() error = %v", err)
		}
		if receipt == nil {
			t.Fatalf("StoreInboundChunk() receipt = nil")
		}
		if i < total-1 && assembled != nil {
			t.Fatalf("StoreInboundChunk() assembled request too early at chunk %d", i)
		}
		if assembled != nil {
			reassembled = assembled
		}
	}

	if reassembled == nil {
		t.Fatalf("reassembled request = nil")
	}
	if reassembled.ID != req.ID || reassembled.Request.URL != req.Request.URL || reassembled.Request.Method != req.Request.Method {
		t.Fatalf("reassembled request = %+v, want %+v", reassembled, req)
	}

	receipts, err := s.ListChunkReceipts(ctx)
	if err != nil {
		t.Fatalf("ListChunkReceipts() error = %v", err)
	}
	if len(receipts) != 1 || !receipts[0].Completed {
		t.Fatalf("ListChunkReceipts() = %+v, want one completed receipt", receipts)
	}
}

func TestSQLiteClearPeerQueueKeepsLeasedRequests(t *testing.T) {
	s := newTestSQLite(t)
	ctx := context.Background()

	queuedReq := &models.RelayRequest{
		ID:        "req-clear-queued",
		Request:   models.HTTPRequest{URL: "http://127.0.0.1:8080/queued", Method: "GET"},
		Status:    models.StatusQueued,
		CreatedAt: time.Now(),
	}
	leasedReq := &models.RelayRequest{
		ID:        "req-clear-leased",
		Request:   models.HTTPRequest{URL: "http://127.0.0.1:8080/leased", Method: "GET"},
		Status:    models.StatusQueued,
		CreatedAt: time.Now(),
	}
	if err := s.EnqueueRequest(ctx, "peer-clear", queuedReq); err != nil {
		t.Fatalf("EnqueueRequest(queued) error = %v", err)
	}
	if err := s.EnqueueRequest(ctx, "peer-clear", leasedReq); err != nil {
		t.Fatalf("EnqueueRequest(leased) error = %v", err)
	}

	leased, err := s.LeaseRequests(ctx, "peer-clear", 1, time.Minute)
	if err != nil {
		t.Fatalf("LeaseRequests() error = %v", err)
	}
	if len(leased) != 1 {
		t.Fatalf("LeaseRequests() len = %d, want 1", len(leased))
	}

	cleared, err := s.ClearPeerQueue(ctx, "peer-clear")
	if err != nil {
		t.Fatalf("ClearPeerQueue() error = %v", err)
	}
	if cleared != 1 {
		t.Fatalf("ClearPeerQueue() = %d, want 1", cleared)
	}

	clearedID := queuedReq.ID
	leasedID := leased[0].ID
	if leasedID == queuedReq.ID {
		clearedID = leasedReq.ID
	}

	statusQueued, err := s.GetRequestStatus(ctx, clearedID)
	if err != nil {
		t.Fatalf("GetRequestStatus(queued) error = %v", err)
	}
	if statusQueued != models.StatusFailed {
		t.Fatalf("queued request status = %s, want %s", statusQueued, models.StatusFailed)
	}

	statusLeased, err := s.GetRequestStatus(ctx, leasedID)
	if err != nil {
		t.Fatalf("GetRequestStatus(leased) error = %v", err)
	}
	if statusLeased != models.StatusLeased {
		t.Fatalf("leased request status = %s, want %s", statusLeased, models.StatusLeased)
	}
}
