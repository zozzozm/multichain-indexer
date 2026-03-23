package indexer

import (
	"context"
	"testing"
	"time"

	"github.com/fystack/multichain-indexer/internal/rpc"
	"github.com/fystack/multichain-indexer/internal/rpc/stellar"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const stellarMainnetHorizon = "https://horizon.stellar.org"

func newTestStellarLiveIndexer(t *testing.T, kvstore infra.KVStore) (*StellarIndexer, *stellar.Client) {
	t.Helper()

	client := stellar.NewClient(stellarMainnetHorizon, nil, 30*time.Second, nil)
	t.Cleanup(func() {
		_ = client.Close()
	})

	failover := rpc.NewFailover[stellar.StellarAPI](nil)
	require.NoError(t, failover.AddProvider(&rpc.Provider{
		Name:       "stellar-mainnet",
		URL:        stellarMainnetHorizon,
		Network:    "stellar_mainnet",
		ClientType: rpc.ClientTypeREST,
		Client:     client,
		State:      rpc.StateHealthy,
	}))

	return NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		failover,
		kvstore,
		nil,
	), client
}

func findStellarBlockTransaction(t *testing.T, block *types.Block, txHash string, transferIndex string) types.Transaction {
	t.Helper()

	for _, tx := range block.Transactions {
		if tx.TxHash == txHash && tx.TransferIndex == transferIndex {
			return tx
		}
	}
	t.Fatalf("transaction %s with transfer index %s not found in block %d", txHash, transferIndex, block.Number)
	return types.Transaction{}
}

func findStellarPayment(t *testing.T, payments []stellar.Payment, pagingToken string) stellar.Payment {
	t.Helper()

	for _, payment := range payments {
		if payment.PagingToken == pagingToken {
			return payment
		}
	}
	t.Fatalf("payment with paging token %s not found", pagingToken)
	return stellar.Payment{}
}

func findStellarOperation(t *testing.T, operations []stellar.Operation, operationID string) stellar.Operation {
	t.Helper()

	for _, operation := range operations {
		if operation.ID == operationID {
			return operation
		}
	}
	t.Fatalf("operation %s not found", operationID)
	return stellar.Operation{}
}

func TestStellarMainnetPaymentIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		txHash        = "4003ae37c7b5126c364e6890a64792f57b268e1bebb43546e0347ba113d16530"
		transferIndex = "265336753060651009"
		expectedFrom  = "GAMV73I2KDYWDLGO5SNVL5IA6JSGLCALD7XJSXHPUB27MMJNTDQIX3UC"
		expectedTo    = "GBN32NH6TMWE4ZD4G245CF3UVOQRXD4FK3FDCLZ4DE5642HWFKSLLRMB"
		expectedAmt   = "56.6500000"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kvstore := newMockKVStore()
	idx, client := newTestStellarLiveIndexer(t, kvstore)

	txDetail, err := client.GetTransaction(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, txDetail)

	ledger, err := client.GetLedger(ctx, txDetail.Ledger)
	require.NoError(t, err)
	require.NotNil(t, ledger)

	payments, err := fetchStellarLedgerPayments(ctx, client, txDetail.Ledger)
	require.NoError(t, err)
	payment := findStellarPayment(t, payments, transferIndex)

	ledgerTimestamp, err := parseRFC3339Unix(ledger.ClosedAt)
	require.NoError(t, err)

	tx, ok := idx.convertPayment(payment, txDetail, ledger.Sequence, ledger.Hash, transferIndex, ledgerTimestamp)
	require.True(t, ok)
	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, expectedTo, tx.ToAddress)
	assert.Equal(t, expectedAmt, tx.Amount)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	assert.Equal(t, formatStellarAsset(payment.AssetIssuer, payment.AssetCode), tx.AssetAddress)
	assert.Equal(t, ledger.Hash, tx.BlockHash)
	assert.Equal(t, transferIndex, tx.TransferIndex)
}

func TestStellarMainnetCreateClaimableBalanceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		txHash            = "2308f9725aafde7aa6249e10abb1ccd3218116c6ee8d56305977db1cdc30319f"
		operationID       = "265336628506390529"
		transferIndex     = "265336628506390529"
		expectedFrom      = "GCKIK5UOKKGXCDCWGY3BAKJ2T5H6BXOXHI4SMMEUSUE6NWLJP3F2KQUU"
		expectedBalanceID = "000000009289e60febbca7863b48b8cf4d68b514d779ef13d1078b2fcd6d9371b0c3647a"
		expectedAmount    = "0.0010000"
		expectedAsset     = "GBZH36ATUXJZKFRMQTAAW42MWNM34SOA4N6E7DQ62V3G5NVITC3QOVRL:OVRL"
		expectedSubtype   = "create_claimable_balance"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kvstore := newMockKVStore()
	idx, client := newTestStellarLiveIndexer(t, kvstore)

	txDetail, err := client.GetTransaction(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, txDetail)

	ledger, err := client.GetLedger(ctx, txDetail.Ledger)
	require.NoError(t, err)
	require.NotNil(t, ledger)

	operations, err := fetchStellarLedgerOperations(ctx, client, txDetail.Ledger)
	require.NoError(t, err)
	operation := findStellarOperation(t, operations, operationID)
	require.Equal(t, transferIndex, operation.PagingToken)

	effects, err := fetchStellarOperationEffects(ctx, client, operationID)
	require.NoError(t, err)

	ledgerTimestamp, err := parseRFC3339Unix(ledger.ClosedAt)
	require.NoError(t, err)

	tx, ok, err := idx.convertOperation(
		operation,
		effects,
		txDetail,
		ledger.Sequence,
		ledger.Hash,
		transferIndex,
		ledgerTimestamp,
	)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, stellarClaimableBalanceAddress(expectedBalanceID), tx.ToAddress)
	assert.Equal(t, expectedAmount, tx.Amount)
	assert.Equal(t, expectedAsset, tx.AssetAddress)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	assert.Equal(t, ledger.Hash, tx.BlockHash)
	assert.Equal(t, transferIndex, tx.TransferIndex)

	subtype := tx.GetMetadataString(types.MetadataKeySubtype)
	assert.Equal(t, expectedSubtype, subtype)

	claimableID := tx.GetMetadataString(types.MetadataKeyClaimableID)
	assert.Equal(t, expectedBalanceID, claimableID)

	state, found, err := idx.loadClaimableBalanceState(expectedBalanceID)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, operation.Asset, state.Asset)
	assert.Equal(t, operation.Amount, state.Amount)
	assert.Equal(t, operation.SourceAccount, state.SourceAccount)
	assert.Equal(t, stellarClaimantAddresses(operation.Claimants), state.Claimants)
}

func TestStellarMainnetClaimClaimableBalanceIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		txHash            = "eaef80df07081e46288947f6e286f25c09ff552ab861b5f90a2cdfbd1976e880"
		operationID       = "265334914814349334"
		transferIndex     = "265334914814349334"
		expectedBalanceID = "0000000022b1a16b0a8d5aa67f932dc7a4f28f0ae44b7b270af4fce16a02362eea7e3897"
		expectedClaimant  = "GCKIK5UOKKGXCDCWGY3BAKJ2T5H6BXOXHI4SMMEUSUE6NWLJP3F2KQUU"
		expectedSubtype   = "claim_claimable_balance"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	kvstore := newMockKVStore()
	idx, client := newTestStellarLiveIndexer(t, kvstore)

	txDetail, err := client.GetTransaction(ctx, txHash)
	require.NoError(t, err)
	require.NotNil(t, txDetail)

	ledger, err := client.GetLedger(ctx, txDetail.Ledger)
	require.NoError(t, err)
	require.NotNil(t, ledger)

	operations, err := fetchStellarLedgerOperations(ctx, client, txDetail.Ledger)
	require.NoError(t, err)
	operation := findStellarOperation(t, operations, operationID)
	require.Equal(t, transferIndex, operation.PagingToken)

	effects, err := fetchStellarOperationEffects(ctx, client, operationID)
	require.NoError(t, err)
	effect := findStellarEffect(effects, "claimable_balance_claimed")
	require.NotNil(t, effect)
	require.Equal(t, expectedBalanceID, effect.BalanceID)

	err = idx.saveClaimableBalanceState(expectedBalanceID, stellarClaimableBalanceState{
		Asset:         effect.Asset,
		Amount:        effect.Amount,
		SourceAccount: expectedClaimant,
		Claimants:     []string{expectedClaimant},
	})
	require.NoError(t, err)

	ledgerTimestamp, err := parseRFC3339Unix(ledger.ClosedAt)
	require.NoError(t, err)

	tx, ok, err := idx.convertOperation(
		operation,
		effects,
		txDetail,
		txDetail.Ledger,
		ledger.Hash,
		transferIndex,
		ledgerTimestamp,
	)
	require.NoError(t, err)
	require.True(t, ok)

	assert.Equal(t, txHash, tx.TxHash)
	assert.Equal(t, stellarClaimableBalanceAddress(expectedBalanceID), tx.FromAddress)
	assert.Equal(t, expectedClaimant, tx.ToAddress)
	assert.Equal(t, effect.Amount, tx.Amount)
	assert.Equal(t, "GBZH36ATUXJZKFRMQTAAW42MWNM34SOA4N6E7DQ62V3G5NVITC3QOVRL:OVRL", tx.AssetAddress)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	assert.Equal(t, ledger.Hash, tx.BlockHash)
	assert.Equal(t, transferIndex, tx.TransferIndex)

	subtype := tx.GetMetadataString(types.MetadataKeySubtype)
	assert.Equal(t, expectedSubtype, subtype)

	claimableID := tx.GetMetadataString(types.MetadataKeyClaimableID)
	assert.Equal(t, expectedBalanceID, claimableID)

	_, found, err := idx.loadClaimableBalanceState(expectedBalanceID)
	require.NoError(t, err)
	assert.False(t, found)
}
