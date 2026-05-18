package transport

import (
	"encoding/base64"
	"testing"
	"time"

	"github.com/relayra/relayra/internal/models"
)

func TestRequestChunkAt(t *testing.T) {
	req := models.RelayRequest{
		ID: "req-transport",
		Request: models.HTTPRequest{
			URL:    "http://127.0.0.1:8080/test",
			Method: "POST",
			Body:   string(make([]byte, 2048)),
		},
		Status:    models.StatusQueued,
		CreatedAt: time.Now(),
	}

	needsChunking, size, err := RequestNeedsChunking(req, 256)
	if err != nil {
		t.Fatalf("RequestNeedsChunking() error = %v", err)
	}
	if !needsChunking || size == 0 {
		t.Fatalf("RequestNeedsChunking() = (%t, %d), want chunked payload", needsChunking, size)
	}

	chunk, err := RequestChunkAt(req, 256, 0)
	if err != nil {
		t.Fatalf("RequestChunkAt() error = %v", err)
	}
	if chunk.TransferID != models.RequestTransferID(req.ID) {
		t.Fatalf("chunk.TransferID = %q, want %q", chunk.TransferID, models.RequestTransferID(req.ID))
	}
	if chunk.Total < 2 {
		t.Fatalf("chunk.Total = %d, want at least 2", chunk.Total)
	}
	if _, err := base64.StdEncoding.DecodeString(chunk.Payload); err != nil {
		t.Fatalf("chunk.Payload is not valid base64: %v", err)
	}
}
