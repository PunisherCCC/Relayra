package store

import (
	"context"
	"database/sql"
	"encoding/base64"
	"encoding/json"
	"fmt"
	"time"

	"github.com/relayra/relayra/internal/models"
)

func (s *SQLite) StoreInboundChunk(ctx context.Context, chunk models.TransportChunk, ttl time.Duration) (*models.ChunkReceipt, *models.RelayRequest, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin inbound chunk tx: %w", err)
	}
	defer tx.Rollback()

	var (
		requestID string
		kind      string
		nextIndex int
		total     int
		checksum  string
		totalSize int
		data      string
	)
	err = tx.QueryRowContext(ctx, `
		SELECT request_id, kind, next_index, total, checksum, total_size, data
		FROM inbound_chunks
		WHERE transfer_id = ? AND expires_at > ?
	`, chunk.TransferID, time.Now().Unix()).Scan(&requestID, &kind, &nextIndex, &total, &checksum, &totalSize, &data)
	if err != nil && err != sql.ErrNoRows {
		return nil, nil, fmt.Errorf("load inbound chunk state: %w", err)
	}

	if err == nil {
		if checksum != chunk.Checksum {
			return s.resetInboundChunk(ctx, chunk, "checksum changed")
		}
		if total != chunk.Total {
			return s.resetInboundChunk(ctx, chunk, "chunk total changed")
		}
	}

	if chunk.Index < nextIndex {
		receipt := models.ChunkReceipt{
			TransferID: chunk.TransferID,
			RequestID:  chunk.RequestID,
			NextIndex:  nextIndex,
			Completed:  nextIndex >= chunk.Total,
		}
		if err := s.storeChunkReceipt(ctx, receipt); err != nil {
			return nil, nil, err
		}
		return &receipt, nil, nil
	}
	if chunk.Index > nextIndex {
		return s.resetInboundChunk(ctx, chunk, fmt.Sprintf("out-of-order chunk index %d expected %d", chunk.Index, nextIndex))
	}

	payloadBytes, err := base64.StdEncoding.DecodeString(chunk.Payload)
	if err != nil {
		return s.resetInboundChunk(ctx, chunk, "invalid chunk payload")
	}

	data += string(payloadBytes)
	nextIndex++
	expiresAt := time.Now().Add(ttl).Unix()
	if _, err := tx.ExecContext(ctx, `
		INSERT INTO inbound_chunks (transfer_id, request_id, kind, next_index, total, checksum, total_size, data, expires_at, updated_at)
		VALUES (?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			request_id = excluded.request_id,
			kind = excluded.kind,
			next_index = excluded.next_index,
			total = excluded.total,
			checksum = excluded.checksum,
			total_size = excluded.total_size,
			data = excluded.data,
			expires_at = excluded.expires_at,
			updated_at = excluded.updated_at
	`, chunk.TransferID, chunk.RequestID, chunk.Kind, nextIndex, chunk.Total, chunk.Checksum, chunk.TotalSize, data, expiresAt, time.Now().Unix()); err != nil {
		return nil, nil, fmt.Errorf("upsert inbound chunk state: %w", err)
	}

	receipt := models.ChunkReceipt{
		TransferID: chunk.TransferID,
		RequestID:  chunk.RequestID,
		NextIndex:  nextIndex,
		Completed:  nextIndex >= chunk.Total,
	}
	if err := s.storeChunkReceiptTx(ctx, tx, receipt); err != nil {
		return nil, nil, err
	}

	if nextIndex < chunk.Total {
		if err := tx.Commit(); err != nil {
			return nil, nil, fmt.Errorf("commit inbound chunk tx: %w", err)
		}
		return &receipt, nil, nil
	}

	if len(data) != chunk.TotalSize {
		return s.resetInboundChunk(ctx, chunk, "reassembled payload size mismatch")
	}
	if models.SHA256Hex([]byte(data)) != chunk.Checksum {
		return s.resetInboundChunk(ctx, chunk, "reassembled payload checksum mismatch")
	}

	var req models.RelayRequest
	if err := json.Unmarshal([]byte(data), &req); err != nil {
		return s.resetInboundChunk(ctx, chunk, "invalid reassembled request payload")
	}

	if _, err := tx.ExecContext(ctx, `DELETE FROM inbound_chunks WHERE transfer_id = ?`, chunk.TransferID); err != nil {
		return nil, nil, fmt.Errorf("delete completed inbound chunk state: %w", err)
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit completed inbound chunk tx: %w", err)
	}

	return &receipt, &req, nil
}

func (s *SQLite) ListChunkReceipts(ctx context.Context) ([]models.ChunkReceipt, error) {
	rows, err := s.db.QueryContext(ctx, `
		SELECT transfer_id, request_id, next_index, completed, reset, error
		FROM chunk_receipts
		ORDER BY transfer_id ASC
	`)
	if err != nil {
		return nil, fmt.Errorf("list chunk receipts: %w", err)
	}
	defer rows.Close()

	var receipts []models.ChunkReceipt
	for rows.Next() {
		var receipt models.ChunkReceipt
		var completed, reset int
		if err := rows.Scan(&receipt.TransferID, &receipt.RequestID, &receipt.NextIndex, &completed, &reset, &receipt.Error); err != nil {
			return nil, err
		}
		receipt.Completed = completed == 1
		receipt.Reset = reset == 1
		receipts = append(receipts, receipt)
	}
	return receipts, rows.Err()
}

func (s *SQLite) DeleteChunkReceipt(ctx context.Context, transferID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM chunk_receipts WHERE transfer_id = ?`, transferID)
	if err != nil {
		return fmt.Errorf("delete chunk receipt %s: %w", transferID, err)
	}
	return nil
}

func (s *SQLite) ApplyChunkReceipt(ctx context.Context, receipt models.ChunkReceipt) error {
	nextIndex := receipt.NextIndex
	if receipt.Reset {
		nextIndex = 0
	}
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO chunk_cursors (transfer_id, request_id, peer_id, next_index, updated_at)
		VALUES (?, ?, '', ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			request_id = excluded.request_id,
			next_index = excluded.next_index,
			updated_at = excluded.updated_at
	`, receipt.TransferID, receipt.RequestID, nextIndex, time.Now().Unix())
	if err != nil {
		return fmt.Errorf("apply chunk receipt %s: %w", receipt.TransferID, err)
	}
	return nil
}

func (s *SQLite) GetChunkCursor(ctx context.Context, transferID string) (*models.ChunkCursor, error) {
	row := s.db.QueryRowContext(ctx, `
		SELECT request_id, peer_id, next_index, updated_at
		FROM chunk_cursors
		WHERE transfer_id = ?
	`, transferID)

	var cursor models.ChunkCursor
	var peerID sql.NullString
	var updatedAt int64
	if err := row.Scan(&cursor.RequestID, &peerID, &cursor.NextIndex, &updatedAt); err != nil {
		if err == sql.ErrNoRows {
			return nil, nil
		}
		return nil, fmt.Errorf("get chunk cursor %s: %w", transferID, err)
	}

	cursor.TransferID = transferID
	cursor.PeerID = peerID.String
	cursor.UpdatedAt = time.Unix(updatedAt, 0)
	return &cursor, nil
}

func (s *SQLite) DeleteChunkCursor(ctx context.Context, transferID string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM chunk_cursors WHERE transfer_id = ?`, transferID)
	if err != nil {
		return fmt.Errorf("delete chunk cursor %s: %w", transferID, err)
	}
	return nil
}

func (s *SQLite) resetInboundChunk(ctx context.Context, chunk models.TransportChunk, reason string) (*models.ChunkReceipt, *models.RelayRequest, error) {
	tx, err := s.db.BeginTx(ctx, nil)
	if err != nil {
		return nil, nil, fmt.Errorf("begin inbound chunk reset tx: %w", err)
	}
	defer tx.Rollback()

	if _, err := tx.ExecContext(ctx, `DELETE FROM inbound_chunks WHERE transfer_id = ?`, chunk.TransferID); err != nil {
		return nil, nil, fmt.Errorf("delete inbound chunk state: %w", err)
	}

	receipt := models.ChunkReceipt{
		TransferID: chunk.TransferID,
		RequestID:  chunk.RequestID,
		NextIndex:  0,
		Reset:      true,
		Error:      reason,
	}
	if err := s.storeChunkReceiptTx(ctx, tx, receipt); err != nil {
		return nil, nil, err
	}
	if err := tx.Commit(); err != nil {
		return nil, nil, fmt.Errorf("commit inbound chunk reset tx: %w", err)
	}
	return &receipt, nil, nil
}

func (s *SQLite) storeChunkReceipt(ctx context.Context, receipt models.ChunkReceipt) error {
	_, err := s.db.ExecContext(ctx, `
		INSERT INTO chunk_receipts (transfer_id, request_id, next_index, completed, reset, error)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			request_id = excluded.request_id,
			next_index = excluded.next_index,
			completed = excluded.completed,
			reset = excluded.reset,
			error = excluded.error
	`, receipt.TransferID, receipt.RequestID, receipt.NextIndex, boolToInt(receipt.Completed), boolToInt(receipt.Reset), receipt.Error)
	if err != nil {
		return fmt.Errorf("store chunk receipt %s: %w", receipt.TransferID, err)
	}
	return nil
}

func (s *SQLite) storeChunkReceiptTx(ctx context.Context, tx *sql.Tx, receipt models.ChunkReceipt) error {
	_, err := tx.ExecContext(ctx, `
		INSERT INTO chunk_receipts (transfer_id, request_id, next_index, completed, reset, error)
		VALUES (?, ?, ?, ?, ?, ?)
		ON CONFLICT(transfer_id) DO UPDATE SET
			request_id = excluded.request_id,
			next_index = excluded.next_index,
			completed = excluded.completed,
			reset = excluded.reset,
			error = excluded.error
	`, receipt.TransferID, receipt.RequestID, receipt.NextIndex, boolToInt(receipt.Completed), boolToInt(receipt.Reset), receipt.Error)
	if err != nil {
		return fmt.Errorf("store chunk receipt %s: %w", receipt.TransferID, err)
	}
	return nil
}
