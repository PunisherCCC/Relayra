package transport

import (
	"encoding/base64"
	"encoding/json"
	"fmt"

	"github.com/relayra/relayra/internal/models"
)

// RequestPayload marshals a relay request into its transport payload bytes.
func RequestPayload(req models.RelayRequest) ([]byte, error) {
	data, err := json.Marshal(req)
	if err != nil {
		return nil, fmt.Errorf("marshal relay request %s: %w", req.ID, err)
	}
	return data, nil
}

// RequestNeedsChunking reports whether the request exceeds the configured chunk size.
func RequestNeedsChunking(req models.RelayRequest, chunkSize int) (bool, int, error) {
	data, err := RequestPayload(req)
	if err != nil {
		return false, 0, err
	}
	return len(data) > chunkSize, len(data), nil
}

// RequestChunkAt returns the chunk envelope for a specific request chunk index.
func RequestChunkAt(req models.RelayRequest, chunkSize int, index int) (*models.TransportChunk, error) {
	if chunkSize < 1 {
		return nil, fmt.Errorf("chunk size must be positive")
	}

	data, err := RequestPayload(req)
	if err != nil {
		return nil, err
	}

	total := (len(data) + chunkSize - 1) / chunkSize
	if total == 0 {
		total = 1
	}
	if index < 0 || index >= total {
		return nil, fmt.Errorf("chunk index %d out of range for %d chunk(s)", index, total)
	}

	start := index * chunkSize
	end := start + chunkSize
	if end > len(data) {
		end = len(data)
	}

	return &models.TransportChunk{
		TransferID: models.RequestTransferID(req.ID),
		RequestID:  req.ID,
		Kind:       models.TransportChunkKindRequest,
		Index:      index,
		Total:      total,
		Payload:    base64.StdEncoding.EncodeToString(data[start:end]),
		Checksum:   models.SHA256Hex(data),
		TotalSize:  len(data),
	}, nil
}
