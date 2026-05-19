package transport

import (
	"context"
	"encoding/json"
	"fmt"
	"time"

	"github.com/relayra/relayra/internal/models"
	"github.com/relayra/relayra/internal/store"
)

func EnqueueWSMessage(ctx context.Context, rdb store.Backend, scope string, msg *models.WSMessage, refID string) (int64, error) {
	if msg == nil {
		return 0, fmt.Errorf("websocket message is required")
	}
	seq, err := rdb.NextWSOutboundSeq(ctx, scope)
	if err != nil {
		return 0, err
	}
	msg.Seq = seq
	msg.SentAt = time.Now().UnixMilli()
	payload, err := json.Marshal(msg)
	if err != nil {
		return 0, fmt.Errorf("marshal websocket message: %w", err)
	}
	if err := rdb.EnqueueWSOutbox(ctx, scope, seq, msg.Type, refID, string(payload)); err != nil {
		return 0, err
	}
	return seq, nil
}

func DecodeWSOutboxMessage(entry models.WSOutboxMessage) (*models.WSMessage, error) {
	var msg models.WSMessage
	if err := json.Unmarshal([]byte(entry.Payload), &msg); err != nil {
		return nil, fmt.Errorf("decode websocket outbox message %d: %w", entry.Seq, err)
	}
	return &msg, nil
}
