package stellar

import (
	"context"

	"github.com/fystack/multichain-indexer/internal/rpc"
)

type StellarAPI interface {
	rpc.NetworkClient

	GetLatestLedgerSequence(ctx context.Context) (uint64, error)
	GetLedger(ctx context.Context, sequence uint64) (*Ledger, error)
	GetPaymentsByLedger(ctx context.Context, sequence uint64, cursor string, limit int) (*PaymentsPage, error)
	GetOperationsByLedger(ctx context.Context, sequence uint64, cursor string, limit int) (*OperationsPage, error)
	GetEffectsByOperation(ctx context.Context, operationID string, cursor string, limit int) (*EffectsPage, error)
	GetTransaction(ctx context.Context, hash string) (*Transaction, error)
}
