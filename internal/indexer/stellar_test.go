package indexer

import (
	"context"
	"encoding/json"
	"sync"
	"testing"

	"github.com/fystack/multichain-indexer/internal/rpc"
	"github.com/fystack/multichain-indexer/internal/rpc/stellar"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/fystack/multichain-indexer/pkg/infra"
	"github.com/hashicorp/consul/api"
	"github.com/stretchr/testify/assert"
	"github.com/stretchr/testify/require"
)

type mockStellarPubkeyStore struct {
	addresses map[string]struct{}
}

func (m mockStellarPubkeyStore) Exist(_ enum.NetworkType, address string) bool {
	_, ok := m.addresses[address]
	return ok
}

type mockStellarAPI struct {
	mu           sync.Mutex
	latest       uint64
	ledgers      map[uint64]*stellar.Ledger
	pages        map[uint64]map[string]*stellar.PaymentsPage
	operations   map[uint64]map[string]*stellar.OperationsPage
	effects      map[string]map[string]*stellar.EffectsPage
	transactions map[string]*stellar.Transaction
	txCalls      map[string]int
	effectCalls  map[string]int
}

var _ stellar.StellarAPI = (*mockStellarAPI)(nil)

func (m *mockStellarAPI) CallRPC(context.Context, string, any) (*rpc.RPCResponse, error) {
	return nil, nil
}
func (m *mockStellarAPI) Do(context.Context, string, string, any, map[string]string) ([]byte, error) {
	return nil, nil
}
func (m *mockStellarAPI) GetNetworkType() string { return rpc.NetworkStellar }
func (m *mockStellarAPI) GetClientType() string  { return rpc.ClientTypeREST }
func (m *mockStellarAPI) GetURL() string         { return "https://stellar.test" }
func (m *mockStellarAPI) Close() error           { return nil }

func (m *mockStellarAPI) GetLatestLedgerSequence(context.Context) (uint64, error) {
	return m.latest, nil
}
func (m *mockStellarAPI) GetLedger(_ context.Context, sequence uint64) (*stellar.Ledger, error) {
	return m.ledgers[sequence], nil
}
func (m *mockStellarAPI) GetPaymentsByLedger(_ context.Context, sequence uint64, cursor string, limit int) (*stellar.PaymentsPage, error) {
	if pageSet, ok := m.pages[sequence]; ok {
		return pageSet[cursor], nil
	}
	return nil, nil
}
func (m *mockStellarAPI) GetOperationsByLedger(_ context.Context, sequence uint64, cursor string, limit int) (*stellar.OperationsPage, error) {
	if pageSet, ok := m.operations[sequence]; ok {
		return pageSet[cursor], nil
	}
	return nil, nil
}
func (m *mockStellarAPI) GetEffectsByOperation(_ context.Context, operationID string, cursor string, limit int) (*stellar.EffectsPage, error) {
	m.mu.Lock()
	if m.effectCalls != nil {
		m.effectCalls[operationID]++
	}
	m.mu.Unlock()
	if pageSet, ok := m.effects[operationID]; ok {
		return pageSet[cursor], nil
	}
	return nil, nil
}
func (m *mockStellarAPI) GetTransaction(_ context.Context, hash string) (*stellar.Transaction, error) {
	m.mu.Lock()
	if m.txCalls != nil {
		m.txCalls[hash]++
	}
	m.mu.Unlock()
	return m.transactions[hash], nil
}

type mockKVStore struct {
	mu    sync.RWMutex
	items map[string][]byte
}

func newMockKVStore() *mockKVStore {
	return &mockKVStore{items: make(map[string][]byte)}
}

func (m *mockKVStore) GetName() string { return "mock" }

func (m *mockKVStore) Set(k string, v string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[k] = []byte(v)
	return nil
}

func (m *mockKVStore) Get(k string) (string, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	val, ok := m.items[k]
	if !ok {
		return "", nil
	}
	return string(val), nil
}

func (m *mockKVStore) GetWithOptions(k string, _ *api.QueryOptions) (string, error) {
	return m.Get(k)
}

func (m *mockKVStore) SetAny(k string, v any) error {
	data, err := json.Marshal(v)
	if err != nil {
		return err
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	m.items[k] = data
	return nil
}

func (m *mockKVStore) GetAny(k string, v any) (bool, error) {
	m.mu.RLock()
	data, ok := m.items[k]
	m.mu.RUnlock()
	if !ok {
		return false, nil
	}
	return true, json.Unmarshal(data, v)
}

func (m *mockKVStore) List(prefix string) ([]*infra.KVPair, error) {
	m.mu.RLock()
	defer m.mu.RUnlock()
	result := make([]*infra.KVPair, 0)
	for key, value := range m.items {
		if len(prefix) == 0 || key == prefix || (len(key) > len(prefix) && key[:len(prefix)] == prefix) {
			result = append(result, &infra.KVPair{Key: key, Value: value})
		}
	}
	return result, nil
}

func (m *mockKVStore) Delete(k string) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	delete(m.items, k)
	return nil
}

func (m *mockKVStore) BatchSet(pairs []infra.KVPair) error {
	m.mu.Lock()
	defer m.mu.Unlock()
	for _, pair := range pairs {
		m.items[pair.Key] = pair.Value
	}
	return nil
}

func (m *mockKVStore) Close() error { return nil }

func TestStellarGetBlock_ParsesNativeAndIssuedPaymentsWithMemo(t *testing.T) {
	t.Parallel()

	api := &mockStellarAPI{
		latest: 15,
		ledgers: map[uint64]*stellar.Ledger{
			10: {
				Hash:     "LEDGER_HASH",
				PrevHash: "PARENT_HASH",
				Sequence: 10,
				ClosedAt: "2026-03-18T10:00:00Z",
			},
		},
		pages: map[uint64]map[string]*stellar.PaymentsPage{
			10: {
				"": {
					Embedded: struct {
						Records []stellar.Payment `json:"records"`
					}{
						Records: []stellar.Payment{
							{
								ID:                    "1",
								PagingToken:           "pt1",
								Type:                  "payment",
								TransactionHash:       "tx-native",
								TransactionSuccessful: true,
								From:                  "GFROM",
								To:                    "GDEST",
								Amount:                "12.5000000",
								AssetType:             "native",
								CreatedAt:             "2026-03-18T10:00:00Z",
							},
							{
								ID:                    "2",
								PagingToken:           "pt2",
								Type:                  "payment",
								TransactionHash:       "tx-token",
								TransactionSuccessful: true,
								From:                  "GISSUERFROM",
								To:                    "GDEST",
								Amount:                "99.2500000",
								AssetType:             "credit_alphanum4",
								AssetCode:             "USDC",
								AssetIssuer:           "GISSUER",
								CreatedAt:             "2026-03-18T10:00:02Z",
							},
							{
								ID:                    "3",
								PagingToken:           "pt3",
								Type:                  "create_account",
								TransactionHash:       "tx-create",
								TransactionSuccessful: true,
								Funder:                "GCREATOR",
								Account:               "GDEST",
								StartingBalance:       "10000.0000000",
								CreatedAt:             "2026-03-18T10:00:03Z",
							},
							{
								ID:                    "4",
								PagingToken:           "pt4",
								Type:                  "path_payment_strict_send",
								TransactionHash:       "tx-path",
								TransactionSuccessful: true,
								From:                  "GPATHFROM",
								To:                    "GDEST",
								Amount:                "7.1250000",
								AssetType:             "native",
								CreatedAt:             "2026-03-18T10:00:04Z",
							},
						},
					},
				},
			},
		},
		transactions: map[string]*stellar.Transaction{
			"tx-native": {
				Hash:       "tx-native",
				Successful: true,
				FeeCharged: "100",
				Memo:       "memo-1",
				MemoType:   "text",
				Ledger:     10,
				CreatedAt:  "2026-03-18T10:00:00Z",
			},
			"tx-token": {
				Hash:       "tx-token",
				Successful: true,
				FeeCharged: "200",
				Memo:       "memo-2",
				MemoType:   "text",
				Ledger:     10,
				CreatedAt:  "2026-03-18T10:00:02Z",
			},
			"tx-create": {
				Hash:       "tx-create",
				Successful: true,
				FeeCharged: "100",
				Memo:       "",
				MemoType:   "none",
				Ledger:     10,
				CreatedAt:  "2026-03-18T10:00:03Z",
			},
			"tx-path": {
				Hash:       "tx-path",
				Successful: true,
				FeeCharged: "300",
				Memo:       "memo-3",
				MemoType:   "text",
				Ledger:     10,
				CreatedAt:  "2026-03-18T10:00:04Z",
			},
		},
	}

	failover := rpc.NewFailover[stellar.StellarAPI](nil)
	require.NoError(t, failover.AddProvider(&rpc.Provider{
		Name:       "stellar-test",
		URL:        "https://stellar.test",
		Network:    "stellar_mainnet",
		ClientType: rpc.ClientTypeREST,
		Client:     api,
		State:      rpc.StateHealthy,
	}))

	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		failover,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GDEST": {}}},
	)

	block, err := idx.GetBlock(context.Background(), 10)
	require.NoError(t, err)
	require.Len(t, block.Transactions, 4)

	nativeTx := block.Transactions[0]
	assert.Equal(t, constant.TxTypeNativeTransfer, nativeTx.Type)
	assert.Equal(t, "", nativeTx.AssetAddress)
	assert.Equal(t, "LEDGER_HASH", nativeTx.BlockHash)
	assert.Equal(t, "pt1", nativeTx.TransferIndex)
	memo := nativeTx.GetMetadataString(types.MetadataKeyMemo)
	assert.Equal(t, "memo-1", memo)
	memoType := nativeTx.GetMetadataString(types.MetadataKeyMemoType)
	assert.Equal(t, "text", memoType)
	assert.Equal(t, "0.00001", nativeTx.TxFee.String())

	tokenTx := block.Transactions[1]
	assert.Equal(t, constant.TxTypeTokenTransfer, tokenTx.Type)
	assert.Equal(t, "GISSUER:USDC", tokenTx.AssetAddress)
	assert.Equal(t, "LEDGER_HASH", tokenTx.BlockHash)
	assert.Equal(t, "pt2", tokenTx.TransferIndex)
	memo = tokenTx.GetMetadataString(types.MetadataKeyMemo)
	assert.Equal(t, "memo-2", memo)
	assert.Equal(t, "99.2500000", tokenTx.Amount)

	createTx := block.Transactions[2]
	assert.Equal(t, constant.TxTypeNativeTransfer, createTx.Type)
	assert.Equal(t, "LEDGER_HASH", createTx.BlockHash)
	assert.Equal(t, "pt3", createTx.TransferIndex)
	assert.Equal(t, "GCREATOR", createTx.FromAddress)
	assert.Equal(t, "GDEST", createTx.ToAddress)
	assert.Equal(t, "10000.0000000", createTx.Amount)

	pathTx := block.Transactions[3]
	assert.Equal(t, constant.TxTypeNativeTransfer, pathTx.Type)
	assert.Equal(t, "LEDGER_HASH", pathTx.BlockHash)
	assert.Equal(t, "pt4", pathTx.TransferIndex)
	assert.Equal(t, "GPATHFROM", pathTx.FromAddress)
	assert.Equal(t, "GDEST", pathTx.ToAddress)
	assert.Equal(t, "7.1250000", pathTx.Amount)
	memo = pathTx.GetMetadataString(types.MetadataKeyMemo)
	assert.Equal(t, "memo-3", memo)
	assert.Equal(t, "0.00003", pathTx.TxFee.String())
}

func TestStellarGetBlock_FetchesTransactionDetailsLazily(t *testing.T) {
	t.Parallel()

	api := &mockStellarAPI{
		latest: 20,
		ledgers: map[uint64]*stellar.Ledger{
			20: {
				Hash:     "LEDGER_HASH",
				PrevHash: "PARENT_HASH",
				Sequence: 20,
				ClosedAt: "2026-03-18T11:00:00Z",
			},
		},
		pages: map[uint64]map[string]*stellar.PaymentsPage{
			20: {
				"": {
					Embedded: struct {
						Records []stellar.Payment `json:"records"`
					}{
						Records: []stellar.Payment{
							{
								ID:                    "1",
								PagingToken:           "pt1",
								Type:                  "payment",
								TransactionHash:       "tx-pay",
								TransactionSuccessful: true,
								From:                  "GFROM",
								To:                    "GDEST",
								Amount:                "1.0000000",
								AssetType:             "native",
							},
							{
								ID:                    "2",
								PagingToken:           "pt2",
								Type:                  "payment",
								TransactionHash:       "tx-pay",
								TransactionSuccessful: true,
								From:                  "GFROM2",
								To:                    "GDEST",
								Amount:                "2.0000000",
								AssetType:             "native",
							},
						},
					},
				},
			},
		},
		operations: map[uint64]map[string]*stellar.OperationsPage{
			20: {
				"": {
					Embedded: struct {
						Records []stellar.Operation `json:"records"`
					}{
						Records: []stellar.Operation{
							{
								ID:                    "op-ignored",
								PagingToken:           "op-ignored",
								Type:                  "manage_sell_offer",
								TransactionHash:       "tx-ignored",
								TransactionSuccessful: true,
							},
							{
								ID:                    "op-create",
								PagingToken:           "op-create",
								Type:                  "create_claimable_balance",
								TransactionHash:       "tx-create",
								TransactionSuccessful: true,
								SourceAccount:         "GSOURCE",
								Asset:                 "USD:GISSUER",
								Amount:                "5.5000000",
								Claimants: []stellar.Claimant{
									{Destination: "GCLAIMANT"},
								},
							},
						},
					},
				},
			},
		},
		effects: map[string]map[string]*stellar.EffectsPage{
			"op-create": {
				"": {
					Embedded: struct {
						Records []stellar.Effect `json:"records"`
					}{
						Records: []stellar.Effect{
							{
								Type:      "claimable_balance_created",
								BalanceID: "balance-1",
								Asset:     "USD:GISSUER",
								Amount:    "5.5000000",
							},
						},
					},
				},
			},
		},
		transactions: map[string]*stellar.Transaction{
			"tx-pay": {
				Hash:       "tx-pay",
				Successful: true,
				FeeCharged: "100",
				Ledger:     20,
				CreatedAt:  "2026-03-18T11:00:01Z",
			},
			"tx-create": {
				Hash:       "tx-create",
				Successful: true,
				FeeCharged: "200",
				Ledger:     20,
				CreatedAt:  "2026-03-18T11:00:02Z",
			},
			"tx-ignored": {
				Hash:       "tx-ignored",
				Successful: true,
				FeeCharged: "300",
				Ledger:     20,
				CreatedAt:  "2026-03-18T11:00:03Z",
			},
		},
		txCalls:     map[string]int{},
		effectCalls: map[string]int{},
	}

	failover := rpc.NewFailover[stellar.StellarAPI](nil)
	require.NoError(t, failover.AddProvider(&rpc.Provider{
		Name:       "stellar-test",
		URL:        "https://stellar.test",
		Network:    "stellar_mainnet",
		ClientType: rpc.ClientTypeREST,
		Client:     api,
		State:      rpc.StateHealthy,
	}))

	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		failover,
		newMockKVStore(),
		mockStellarPubkeyStore{addresses: map[string]struct{}{
			"GDEST":     {},
			"GCLAIMANT": {},
		}},
	)

	block, err := idx.GetBlock(context.Background(), 20)
	require.NoError(t, err)
	require.NotNil(t, block)
	require.Len(t, block.Transactions, 3)

	assert.Equal(t, 1, api.txCalls["tx-pay"])
	assert.Equal(t, 1, api.txCalls["tx-create"])
	assert.Zero(t, api.txCalls["tx-ignored"])
	assert.Equal(t, 1, api.effectCalls["op-create"])
}

func TestFetchStellarLedgerPayments_PaginatesWithoutDuplication(t *testing.T) {
	t.Parallel()

	api := &mockStellarAPI{
		pages: map[uint64]map[string]*stellar.PaymentsPage{
			11: {
				"": {
					Embedded: struct {
						Records []stellar.Payment `json:"records"`
					}{
						Records: []stellar.Payment{
							{PagingToken: "p1", TransactionHash: "tx1"},
							{PagingToken: "p2", TransactionHash: "tx2"},
						},
					},
				},
				"p2": {
					Embedded: struct {
						Records []stellar.Payment `json:"records"`
					}{
						Records: []stellar.Payment{
							{PagingToken: "p3", TransactionHash: "tx3"},
						},
					},
				},
			},
		},
	}

	payments, err := fetchStellarLedgerPayments(context.Background(), api, 11)
	require.NoError(t, err)
	require.Len(t, payments, 3)
	assert.Equal(t, "tx1", payments[0].TransactionHash)
	assert.Equal(t, "tx2", payments[1].TransactionHash)
	assert.Equal(t, "tx3", payments[2].TransactionHash)
}

func TestStellarConvertPayment_RespectsTwoWayIndexing(t *testing.T) {
	t.Parallel()

	payment := stellar.Payment{
		Type:                  "payment",
		TransactionHash:       "tx-out",
		TransactionSuccessful: true,
		From:                  "GMONITORED",
		To:                    "GOTHER",
		Amount:                "1.0000000",
		AssetType:             "native",
	}

	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		nil,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GMONITORED": {}}},
	)
	_, ok := idx.convertPayment(payment, nil, 12, "ledger-hash", "payment-0", 1)
	require.False(t, ok)

	idx = NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet", TwoWayIndexing: true},
		nil,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GMONITORED": {}}},
	)
	_, ok = idx.convertPayment(payment, nil, 12, "ledger-hash", "payment-0", 1)
	require.True(t, ok)
}

func TestStellarConvertPayment_CreateAccountUsesFunderAndStartingBalance(t *testing.T) {
	t.Parallel()

	payment := stellar.Payment{
		Type:                  "create_account",
		TransactionHash:       "tx-create",
		TransactionSuccessful: true,
		Funder:                "GFUNDER",
		Account:               "GNEW",
		StartingBalance:       "10000.0000000",
	}

	idx := NewStellarIndexer(
		"stellar_testnet",
		config.ChainConfig{NetworkId: "stellar_testnet"},
		nil,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GNEW": {}}},
	)

	tx, ok := idx.convertPayment(payment, nil, 42, "ledger-hash", "payment-1", 123)
	require.True(t, ok)
	assert.Equal(t, "ledger-hash", tx.BlockHash)
	assert.Equal(t, "payment-1", tx.TransferIndex)
	assert.Equal(t, "GFUNDER", tx.FromAddress)
	assert.Equal(t, "GNEW", tx.ToAddress)
	assert.Equal(t, "10000.0000000", tx.Amount)
	assert.Equal(t, constant.TxTypeNativeTransfer, tx.Type)
}

func TestStellarConvertPayment_AccountMergeUsesIntoAndAmount(t *testing.T) {
	t.Parallel()

	payment := stellar.Payment{
		Type:                  "account_merge",
		TransactionHash:       "tx-merge",
		TransactionSuccessful: true,
		Account:               "GMERGED",
		Into:                  "GDEST",
		Amount:                "8.5000000",
	}

	idx := NewStellarIndexer(
		"stellar_testnet",
		config.ChainConfig{NetworkId: "stellar_testnet"},
		nil,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GDEST": {}}},
	)

	tx, ok := idx.convertPayment(payment, nil, 52, "ledger-hash", "payment-2", 123)
	require.True(t, ok)
	assert.Equal(t, "ledger-hash", tx.BlockHash)
	assert.Equal(t, "payment-2", tx.TransferIndex)
	assert.Equal(t, "GMERGED", tx.FromAddress)
	assert.Equal(t, "GDEST", tx.ToAddress)
	assert.Equal(t, "8.5000000", tx.Amount)
	assert.Equal(t, constant.TxTypeNativeTransfer, tx.Type)
}

func TestStellarConvertOperation_ClawbackUsesBurnAddress(t *testing.T) {
	t.Parallel()

	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		nil,
		nil,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GHOLDER": {}}},
	)

	tx, ok, err := idx.convertOperation(
		stellar.Operation{
			ID:                    "10",
			PagingToken:           "10",
			Type:                  "clawback",
			TransactionHash:       "tx-claw",
			TransactionSuccessful: true,
			From:                  "GHOLDER",
			Asset:                 "USDC:GISSUER",
			Amount:                "5.0000000",
			CreatedAt:             "2026-03-18T10:00:00Z",
		},
		nil,
		&stellar.Transaction{FeeCharged: "100"},
		77,
		"ledger-hash",
		"10",
		123,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "GHOLDER", tx.FromAddress)
	assert.Equal(t, stellarBurnAddress, tx.ToAddress)
	assert.Equal(t, "GISSUER:USDC", tx.AssetAddress)
	assert.Equal(t, "5.0000000", tx.Amount)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	assert.Equal(t, "ledger-hash", tx.BlockHash)
	assert.Equal(t, "10", tx.TransferIndex)
	subtype := tx.GetMetadataString(types.MetadataKeySubtype)
	assert.Equal(t, "clawback", subtype)
}

func TestStellarConvertOperation_CreateClaimableBalancePersistsState(t *testing.T) {
	t.Parallel()

	kv := newMockKVStore()
	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		nil,
		kv,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GCLAIM": {}}},
	)

	tx, ok, err := idx.convertOperation(
		stellar.Operation{
			ID:                    "200",
			PagingToken:           "200",
			Type:                  "create_claimable_balance",
			TransactionHash:       "tx-create-balance",
			TransactionSuccessful: true,
			SourceAccount:         "GFUNDER",
			Asset:                 "USDC:GISSUER",
			Amount:                "11.5000000",
			Claimants:             []stellar.Claimant{{Destination: "GCLAIM"}},
			CreatedAt:             "2026-03-18T10:00:00Z",
		},
		[]stellar.Effect{
			{Type: "claimable_balance_created", BalanceID: "balance-1"},
		},
		&stellar.Transaction{FeeCharged: "100"},
		77,
		"ledger-hash",
		"200",
		123,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, "GFUNDER", tx.FromAddress)
	assert.Equal(t, stellarClaimableBalanceAddress("balance-1"), tx.ToAddress)
	assert.Equal(t, "GISSUER:USDC", tx.AssetAddress)
	assert.Equal(t, "11.5000000", tx.Amount)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)
	subtype := tx.GetMetadataString(types.MetadataKeySubtype)
	assert.Equal(t, "create_claimable_balance", subtype)
	balanceID := tx.GetMetadataString(types.MetadataKeyClaimableID)
	assert.Equal(t, "balance-1", balanceID)

	var state stellarClaimableBalanceState
	found, err := kv.GetAny(idx.claimableBalanceStateKey("balance-1"), &state)
	require.NoError(t, err)
	require.True(t, found)
	assert.Equal(t, "USDC:GISSUER", state.Asset)
	assert.Equal(t, "11.5000000", state.Amount)
	assert.Equal(t, "GFUNDER", state.SourceAccount)
	assert.Equal(t, []string{"GCLAIM"}, state.Claimants)
}

func TestStellarConvertOperation_ClaimClaimableBalanceConsumesState(t *testing.T) {
	t.Parallel()

	kv := newMockKVStore()
	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		nil,
		kv,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GCLAIM": {}}},
	)
	require.NoError(t, idx.saveClaimableBalanceState("balance-2", stellarClaimableBalanceState{
		Asset:         "USDC:GISSUER",
		Amount:        "7.0000000",
		SourceAccount: "GFUNDER",
		Claimants:     []string{"GCLAIM"},
	}))

	tx, ok, err := idx.convertOperation(
		stellar.Operation{
			ID:                    "201",
			PagingToken:           "201",
			Type:                  "claim_claimable_balance",
			TransactionHash:       "tx-claim-balance",
			TransactionSuccessful: true,
			BalanceID:             "balance-2",
			Claimant:              "GCLAIM",
		},
		nil,
		&stellar.Transaction{FeeCharged: "100"},
		78,
		"ledger-hash",
		"201",
		123,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, stellarClaimableBalanceAddress("balance-2"), tx.FromAddress)
	assert.Equal(t, "GCLAIM", tx.ToAddress)
	assert.Equal(t, "GISSUER:USDC", tx.AssetAddress)
	assert.Equal(t, "7.0000000", tx.Amount)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)

	var state stellarClaimableBalanceState
	found, err := kv.GetAny(idx.claimableBalanceStateKey("balance-2"), &state)
	require.NoError(t, err)
	assert.False(t, found)
}

func TestStellarConvertOperation_ClawbackClaimableBalanceConsumesState(t *testing.T) {
	t.Parallel()

	kv := newMockKVStore()
	idx := NewStellarIndexer(
		"stellar_mainnet",
		config.ChainConfig{NetworkId: "stellar_mainnet"},
		nil,
		kv,
		mockStellarPubkeyStore{addresses: map[string]struct{}{"GCLAIM": {}}},
	)
	require.NoError(t, idx.saveClaimableBalanceState("balance-3", stellarClaimableBalanceState{
		Asset:         "USDC:GISSUER",
		Amount:        "3.5000000",
		SourceAccount: "GFUNDER",
		Claimants:     []string{"GCLAIM"},
	}))

	tx, ok, err := idx.convertOperation(
		stellar.Operation{
			ID:                    "202",
			PagingToken:           "202",
			Type:                  "clawback_claimable_balance",
			TransactionHash:       "tx-clawback-balance",
			TransactionSuccessful: true,
			BalanceID:             "balance-3",
		},
		nil,
		&stellar.Transaction{FeeCharged: "100"},
		79,
		"ledger-hash",
		"202",
		123,
	)
	require.NoError(t, err)
	require.True(t, ok)
	assert.Equal(t, stellarClaimableBalanceAddress("balance-3"), tx.FromAddress)
	assert.Equal(t, stellarBurnAddress, tx.ToAddress)
	assert.Equal(t, "GISSUER:USDC", tx.AssetAddress)
	assert.Equal(t, "3.5000000", tx.Amount)
	assert.Equal(t, constant.TxTypeTokenTransfer, tx.Type)

	var state stellarClaimableBalanceState
	found, err := kv.GetAny(idx.claimableBalanceStateKey("balance-3"), &state)
	require.NoError(t, err)
	assert.False(t, found)
}
