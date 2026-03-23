package indexer

import (
	"context"
	"fmt"
	"testing"
	"time"

	"github.com/fystack/multichain-indexer/internal/rpc"
	xrprpc "github.com/fystack/multichain-indexer/internal/rpc/xrp"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

const xrpMainnetRPC = "https://xrplcluster.com"

func newTestXRPLiveIndexer(t *testing.T) (*XRPIndexer, *xrprpc.Client) {
	t.Helper()

	client := xrprpc.NewClient(xrpMainnetRPC, nil, 30*time.Second, nil)
	t.Cleanup(func() {
		_ = client.Close()
	})

	failover := rpc.NewFailover[xrprpc.XRPLAPI](nil)
	require.NoError(t, failover.AddProvider(&rpc.Provider{
		Name:       "xrp-mainnet",
		URL:        xrpMainnetRPC,
		Network:    "xrp_mainnet",
		ClientType: rpc.ClientTypeRPC,
		Client:     client,
		State:      rpc.StateHealthy,
	}))

	return NewXRPIndexer(
		"xrp_mainnet",
		config.ChainConfig{NetworkId: "xrp_mainnet"},
		failover,
		nil,
	), client
}

func findXRPBlockTransaction(t *testing.T, block *types.Block, txHash string) types.Transaction {
	t.Helper()

	for _, tx := range block.Transactions {
		if tx.TxHash == txHash {
			return tx
		}
	}
	t.Fatalf("transaction %s not found in block %d", txHash, block.Number)
	return types.Transaction{}
}

func findXRPLedgerTransactionIndex(t *testing.T, ledger *xrprpc.Ledger, txHash string) string {
	t.Helper()

	for i, tx := range ledger.Transactions {
		if tx.Hash == txHash {
			return fmt.Sprintf("%d", i)
		}
	}
	t.Fatalf("transaction %s not found in ledger %s", txHash, ledger.LedgerIndex.String())
	return ""
}

func TestXRPMainnetPaymentIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		ledgerIndex    = uint64(103056832)
		txHash         = "030944CC6EFC5193312C41A635D9E5CE67B982ABCE3625238F7D91472C60D55C"
		expectedFrom   = "rJrFQCQKgUJwG6bnDLyX33ea4DwWcjk6P3"
		expectedTo     = "rpW6DG5zYZrgvQvo2paEQaM4TqWQ86EGbS"
		expectedAmount = "1.803482"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx, client := newTestXRPLiveIndexer(t)
	ledger, err := client.GetLedgerByIndex(ctx, ledgerIndex)
	require.NoError(t, err)

	block, err := idx.GetBlock(ctx, ledgerIndex)
	require.NoError(t, err)
	require.NotNil(t, block)

	tx := findXRPBlockTransaction(t, block, txHash)
	assert.Equal(t, txHash, tx.TxHash)
	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, expectedTo, tx.ToAddress)
	assert.Equal(t, expectedAmount, tx.Amount)
	assert.Equal(t, constant.TxTypeNativeTransfer, tx.Type)
	assert.Equal(t, "", tx.AssetAddress)
	assert.Equal(t, block.Hash, tx.BlockHash)
	assert.Equal(t, findXRPLedgerTransactionIndex(t, ledger, txHash), tx.TransferIndex)
}

func TestXRPMainnetAccountDeleteIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		ledgerIndex            = uint64(103056689)
		txHash                 = "BDB1F216067FDC16CA70E585900C72CE75450B1D2524269137C2C4CE59CFA5D5"
		expectedFrom           = "rauksASXjkCF13cpjDGA1DCgWWRm4arJu4"
		expectedTo             = "rMkJJ4HHHBRdeHvE2UXx1xPDh4hnuRW9TZ"
		expectedAmount         = "0.899292"
		expectedDestinationTag = "1717742333"
		expectedSubtype        = "account_delete"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx, client := newTestXRPLiveIndexer(t)
	ledger, err := client.GetLedgerByIndex(ctx, ledgerIndex)
	require.NoError(t, err)

	block, err := idx.GetBlock(ctx, ledgerIndex)
	require.NoError(t, err)
	require.NotNil(t, block)

	tx := findXRPBlockTransaction(t, block, txHash)
	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, expectedTo, tx.ToAddress)
	assert.Equal(t, expectedAmount, tx.Amount)
	assert.Equal(t, constant.TxTypeNativeTransfer, tx.Type)
	assert.Equal(t, block.Hash, tx.BlockHash)
	assert.Equal(t, findXRPLedgerTransactionIndex(t, ledger, txHash), tx.TransferIndex)

	subtype, ok := tx.GetMetadataString(types.MetadataKeySubtype)
	require.True(t, ok)
	assert.Equal(t, expectedSubtype, subtype)

	destinationTag, ok := tx.GetMetadataString(types.MetadataKeyDestinationTag)
	require.True(t, ok)
	assert.Equal(t, expectedDestinationTag, destinationTag)
}

func TestXRPMainnetCheckCashIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		ledgerIndex     = uint64(103056610)
		txHash          = "9317A1D9C13A618B8BE74A22D846B6AD70D62364900E8DAA720D79221CFCDF5B"
		expectedFrom    = "rNsvWWTquJ2rWhGcTEx1AEoWte7ZD9VQPe"
		expectedTo      = "ra61uRmrMG1hpVQKhkL6pMwbJqdYcK1FT7"
		expectedAmount  = "4.004482"
		expectedSubtype = "check_cash"
		expectedCheckID = "3C00D77DE88D4C6AA8E3844AA00D68347925D2CB4ABC987E65AAB9D53CDEB4F0"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx, client := newTestXRPLiveIndexer(t)
	ledger, err := client.GetLedgerByIndex(ctx, ledgerIndex)
	require.NoError(t, err)

	block, err := idx.GetBlock(ctx, ledgerIndex)
	require.NoError(t, err)
	require.NotNil(t, block)

	tx := findXRPBlockTransaction(t, block, txHash)
	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, expectedTo, tx.ToAddress)
	assert.Equal(t, expectedAmount, tx.Amount)
	assert.Equal(t, constant.TxTypeNativeTransfer, tx.Type)
	assert.Equal(t, block.Hash, tx.BlockHash)
	assert.Equal(t, findXRPLedgerTransactionIndex(t, ledger, txHash), tx.TransferIndex)

	subtype, ok := tx.GetMetadataString(types.MetadataKeySubtype)
	require.True(t, ok)
	assert.Equal(t, expectedSubtype, subtype)

	checkID, ok := tx.GetMetadataString(types.MetadataKeyCheckID)
	require.True(t, ok)
	assert.Equal(t, expectedCheckID, checkID)
}

func TestXRPMainnetClawbackIntegration(t *testing.T) {
	if testing.Short() {
		t.Skip("skipping integration test")
	}

	const (
		ledgerIndex     = uint64(102983492)
		txHash          = "ADCF519C04EB8AC273EE1C136258D8A62833E2E57AE3A56EB9C6595EB272FB05"
		expectedFrom    = "rLqBaPeR8xe4d1RJ7tBg6iFVyJjpo1rS2T"
		expectedTo      = xrpBurnAddress
		expectedAmount  = "200"
		expectedAsset   = "r4WspQvEcvCjLQz6cDAxTS5vV6rQ9GuKsF:CLW"
		expectedSubtype = "clawback"
	)

	ctx, cancel := context.WithTimeout(context.Background(), 30*time.Second)
	defer cancel()

	idx, client := newTestXRPLiveIndexer(t)
	ledger, err := client.GetLedgerByIndex(ctx, ledgerIndex)
	require.NoError(t, err)

	block, err := idx.GetBlock(ctx, ledgerIndex)
	require.NoError(t, err)
	require.NotNil(t, block)

	tx := findXRPBlockTransaction(t, block, txHash)
	assert.Equal(t, expectedFrom, tx.FromAddress)
	assert.Equal(t, expectedTo, tx.ToAddress)
	assert.Equal(t, expectedAmount, tx.Amount)
	assert.Equal(t, expectedAsset, tx.AssetAddress)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	assert.Equal(t, block.Hash, tx.BlockHash)
	assert.Equal(t, findXRPLedgerTransactionIndex(t, ledger, txHash), tx.TransferIndex)

	subtype, ok := tx.GetMetadataString(types.MetadataKeySubtype)
	require.True(t, ok)
	assert.Equal(t, expectedSubtype, subtype)
}
