package poller

import (
	"context"

	"github.com/relayra/relayra/internal/models"
	"github.com/relayra/relayra/internal/store"
	"github.com/relayra/relayra/internal/transport"
)

func queueSenderRequestStateWS(ctx context.Context, rdb store.Backend, listenerID string, state models.RequestSyncState) error {
	_, err := transport.EnqueueWSMessage(ctx, rdb, models.SenderWSScope(listenerID), &models.WSMessage{
		Type:         models.WSMessageTypeRequestState,
		PeerID:       listenerID,
		RequestState: &state,
	}, state.RequestID)
	return err
}

func queueSenderResultWS(ctx context.Context, rdb store.Backend, listenerID string, result *models.RelayResult) error {
	if result == nil {
		return nil
	}
	_, err := transport.EnqueueWSMessage(ctx, rdb, models.SenderWSScope(listenerID), &models.WSMessage{
		Type:   models.WSMessageTypeResult,
		PeerID: listenerID,
		Result: result,
	}, result.RequestID)
	return err
}

func queueSenderChunkReceiptWS(ctx context.Context, rdb store.Backend, listenerID string, receipt models.ChunkReceipt) error {
	_, err := transport.EnqueueWSMessage(ctx, rdb, models.SenderWSScope(listenerID), &models.WSMessage{
		Type:         models.WSMessageTypeChunkReceipt,
		PeerID:       listenerID,
		ChunkReceipt: &receipt,
	}, receipt.TransferID)
	return err
}
