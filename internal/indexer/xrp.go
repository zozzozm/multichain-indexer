package indexer

import (
	"context"
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/fystack/multichain-indexer/internal/rpc"
	"github.com/fystack/multichain-indexer/internal/rpc/xrp"
	"github.com/fystack/multichain-indexer/pkg/common/config"
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/enum"
	"github.com/fystack/multichain-indexer/pkg/common/types"
	"github.com/shopspring/decimal"
)

const (
	xrpEpochOffset = 946684800
	xrpBurnAddress = "xrp:burn"
)

type XRPIndexer struct {
	chainName   string
	config      config.ChainConfig
	failover    *rpc.Failover[xrp.XRPLAPI]
	pubkeyStore PubkeyStore
}

func NewXRPIndexer(
	chainName string,
	cfg config.ChainConfig,
	failover *rpc.Failover[xrp.XRPLAPI],
	pubkeyStore PubkeyStore,
) *XRPIndexer {
	return &XRPIndexer{
		chainName:   chainName,
		config:      cfg,
		failover:    failover,
		pubkeyStore: pubkeyStore,
	}
}

func (x *XRPIndexer) GetName() string                  { return strings.ToUpper(x.chainName) }
func (x *XRPIndexer) GetNetworkType() enum.NetworkType { return enum.NetworkTypeXRP }
func (x *XRPIndexer) GetNetworkInternalCode() string   { return x.config.InternalCode }

func (x *XRPIndexer) isMonitoredTransfer(from, to string) bool {
	if x.pubkeyStore == nil {
		return true
	}

	if candidate := normalizeXRPAddress(to); candidate != "" && x.pubkeyStore.Exist(enum.NetworkTypeXRP, candidate) {
		return true
	}

	if !x.config.TwoWayIndexing {
		return false
	}

	candidate := normalizeXRPAddress(from)
	return candidate != "" && x.pubkeyStore.Exist(enum.NetworkTypeXRP, candidate)
}

func (x *XRPIndexer) GetLatestBlockNumber(ctx context.Context) (uint64, error) {
	var latest uint64
	err := x.failover.ExecuteWithRetry(ctx, func(client xrp.XRPLAPI) error {
		n, err := client.GetLatestLedgerIndex(ctx)
		latest = n
		return err
	})
	return latest, err
}

func (x *XRPIndexer) GetBlock(ctx context.Context, number uint64) (*types.Block, error) {
	var ledger *xrp.Ledger
	err := x.failover.ExecuteWithRetry(ctx, func(client xrp.XRPLAPI) error {
		l, err := client.GetLedgerByIndex(ctx, number)
		ledger = l
		return err
	})
	if err != nil {
		return nil, fmt.Errorf("get xrp ledger %d failed: %w", number, err)
	}
	if ledger == nil {
		return nil, fmt.Errorf("xrp ledger %d not found", number)
	}
	return x.convertLedger(ledger, number)
}

func (x *XRPIndexer) GetBlocks(ctx context.Context, from, to uint64, isParallel bool) ([]BlockResult, error) {
	if to < from {
		return nil, fmt.Errorf("invalid range: from %d > to %d", from, to)
	}
	blockNumbers := make([]uint64, 0, to-from+1)
	for n := from; n <= to; n++ {
		blockNumbers = append(blockNumbers, n)
	}
	return x.GetBlocksByNumbers(ctx, blockNumbers)
}

func (x *XRPIndexer) GetBlocksByNumbers(ctx context.Context, blockNumbers []uint64) ([]BlockResult, error) {
	if len(blockNumbers) == 0 {
		return nil, nil
	}

	results := make([]BlockResult, len(blockNumbers))
	indexByLedger := make(map[uint64]int, len(blockNumbers))
	for i, ledgerIndex := range blockNumbers {
		indexByLedger[ledgerIndex] = i
	}

	var ledgers map[uint64]*xrp.Ledger
	err := x.failover.ExecuteWithRetry(ctx, func(client xrp.XRPLAPI) error {
		var err error
		ledgers, err = client.BatchGetLedgersByIndex(ctx, blockNumbers)
		return err
	})
	if err == nil {
		for _, ledgerIndex := range blockNumbers {
			ledger := ledgers[ledgerIndex]
			idx := indexByLedger[ledgerIndex]
			if ledger == nil {
				results[idx] = BlockResult{
					Number: ledgerIndex,
					Error:  &Error{ErrorType: ErrorTypeBlockNotFound, Message: "ledger not found"},
				}
				continue
			}
			block, convertErr := x.convertLedger(ledger, ledgerIndex)
			results[idx] = BlockResult{Number: ledgerIndex, Block: block}
			if convertErr != nil {
				results[idx].Error = &Error{ErrorType: classifyXRPError(convertErr), Message: convertErr.Error()}
			}
		}
		return results, firstBlockError(results)
	}

	workers := x.config.Throttle.Concurrency
	if workers <= 0 {
		workers = 1
	}
	workers = min(workers, len(blockNumbers))

	type job struct {
		index int
		num   uint64
	}
	jobs := make(chan job, workers*2)
	var wg sync.WaitGroup

	for i := 0; i < workers; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			for j := range jobs {
				block, getErr := x.GetBlock(ctx, j.num)
				results[j.index] = BlockResult{Number: j.num, Block: block}
				if getErr != nil {
					results[j.index].Error = &Error{
						ErrorType: classifyXRPError(getErr),
						Message:   getErr.Error(),
					}
				}
			}
		}()
	}

	go func() {
		defer close(jobs)
		for i, num := range blockNumbers {
			select {
			case <-ctx.Done():
				return
			case jobs <- job{index: i, num: num}:
			}
		}
	}()

	wg.Wait()
	if ctx.Err() != nil {
		return nil, ctx.Err()
	}
	return results, firstBlockError(results)
}

func (x *XRPIndexer) IsHealthy() bool {
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	_, err := x.GetLatestBlockNumber(ctx)
	return err == nil
}

func (x *XRPIndexer) convertLedger(ledger *xrp.Ledger, fallbackNumber uint64) (*types.Block, error) {
	if ledger == nil {
		return nil, fmt.Errorf("xrp ledger is nil")
	}

	number := fallbackNumber
	if strings.TrimSpace(ledger.LedgerIndex.String()) != "" {
		parsed, err := strconv.ParseUint(ledger.LedgerIndex.String(), 10, 64)
		if err != nil {
			return nil, fmt.Errorf("invalid xrp ledger_index %q: %w", ledger.LedgerIndex.String(), err)
		}
		number = parsed
	}

	timestamp := xrpLedgerTime(ledger.CloseTime)
	txs := make([]types.Transaction, 0, len(ledger.Transactions))
	blockHash := strings.TrimSpace(ledger.LedgerHash)
	for txIndex, tx := range ledger.Transactions {
		converted, ok := x.convertTransaction(tx, number, blockHash, txIndex, timestamp)
		if !ok {
			continue
		}
		txs = append(txs, converted)
	}

	return &types.Block{
		Number:       number,
		Hash:         ledger.LedgerHash,
		ParentHash:   ledger.ParentHash,
		Timestamp:    timestamp,
		Transactions: txs,
	}, nil
}

func (x *XRPIndexer) convertTransaction(
	tx xrp.Transaction,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	meta := tx.Meta
	if meta == nil {
		meta = tx.MetaData
	}
	if meta == nil || !strings.EqualFold(strings.TrimSpace(meta.TransactionResult), "tesSUCCESS") {
		return types.Transaction{}, false
	}

	switch strings.ToLower(strings.TrimSpace(tx.TransactionType)) {
	case "payment":
		return x.convertPaymentTransaction(tx, meta, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	case "accountdelete":
		return x.convertAccountDeleteTransaction(tx, meta, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	case "checkcash":
		return x.convertCheckCashTransaction(tx, meta, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	case "escrowfinish":
		return x.convertEscrowFinishTransaction(tx, meta, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	case "clawback":
		return x.convertClawbackTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	default:
		return types.Transaction{}, false
	}
}

func classifyXRPError(err error) ErrorType {
	if err == nil {
		return ErrorTypeUnknown
	}
	msg := strings.ToLower(err.Error())
	switch {
	case strings.Contains(msg, "not found"):
		return ErrorTypeBlockNotFound
	case strings.Contains(msg, "timeout"):
		return ErrorTypeTimeout
	default:
		return ErrorTypeUnknown
	}
}

func deliveredAmount(meta *xrp.Meta, fallback any) any {
	if meta == nil || meta.DeliveredAmount == nil {
		return fallback
	}
	return meta.DeliveredAmount
}

func (x *XRPIndexer) convertPaymentTransaction(
	tx xrp.Transaction,
	meta *xrp.Meta,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	from := strings.TrimSpace(tx.Account)
	to := strings.TrimSpace(tx.Destination)
	if from == "" || to == "" {
		return types.Transaction{}, false
	}
	if !x.isMonitoredTransfer(from, to) {
		return types.Transaction{}, false
	}

	amount, txType, assetAddress, ok := parseXRPAmount(deliveredAmount(meta, tx.Amount))
	if !ok {
		return types.Transaction{}, false
	}

	result := x.newBaseTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	result.FromAddress = normalizeXRPAddress(from)
	result.ToAddress = normalizeXRPAddress(to)
	result.AssetAddress = assetAddress
	result.Amount = amount
	result.Type = txType
	setMetadataString(&result, types.MetadataKeyDestinationTag, formatDestinationTag(tx.DestinationTag))
	return result, true
}

func (x *XRPIndexer) convertAccountDeleteTransaction(
	tx xrp.Transaction,
	meta *xrp.Meta,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	from := strings.TrimSpace(tx.Account)
	to := strings.TrimSpace(tx.Destination)
	if from == "" || to == "" {
		return types.Transaction{}, false
	}
	if !x.isMonitoredTransfer(from, to) {
		return types.Transaction{}, false
	}

	amount, ok := accountDeleteAmount(meta, to)
	if !ok {
		return types.Transaction{}, false
	}

	result := x.newBaseTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	result.FromAddress = normalizeXRPAddress(from)
	result.ToAddress = normalizeXRPAddress(to)
	result.Amount = amount
	result.Type = constant.TxTypeNativeTransfer
	setMetadataString(&result, types.MetadataKeySubtype, "account_delete")
	setMetadataString(&result, types.MetadataKeyDestinationTag, formatDestinationTag(tx.DestinationTag))
	return result, true
}

func (x *XRPIndexer) convertCheckCashTransaction(
	tx xrp.Transaction,
	meta *xrp.Meta,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	if meta == nil || meta.DeliveredAmount == nil {
		return types.Transaction{}, false
	}

	checkNode := findLedgerNode(meta, "check", func(node *xrp.LedgerNode) bool {
		return strings.EqualFold(strings.TrimSpace(node.LedgerIndex), strings.TrimSpace(tx.CheckID))
	})
	fields := ledgerNodeFields(checkNode)
	if fields == nil {
		return types.Transaction{}, false
	}

	from := strings.TrimSpace(fields.Account)
	to := strings.TrimSpace(tx.Account)
	if from == "" || to == "" {
		return types.Transaction{}, false
	}
	if !x.isMonitoredTransfer(from, to) {
		return types.Transaction{}, false
	}

	amount, txType, assetAddress, ok := parseXRPAmount(meta.DeliveredAmount)
	if !ok {
		return types.Transaction{}, false
	}

	result := x.newBaseTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	result.FromAddress = normalizeXRPAddress(from)
	result.ToAddress = normalizeXRPAddress(to)
	result.AssetAddress = assetAddress
	result.Amount = amount
	result.Type = txType
	setMetadataString(&result, types.MetadataKeySubtype, "check_cash")
	setMetadataString(&result, types.MetadataKeyCheckID, strings.TrimSpace(tx.CheckID))
	setMetadataString(&result, types.MetadataKeyDestinationTag, formatDestinationTag(fields.DestinationTag))
	return result, true
}

func (x *XRPIndexer) convertEscrowFinishTransaction(
	tx xrp.Transaction,
	meta *xrp.Meta,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	owner := normalizeXRPAddress(strings.TrimSpace(tx.Owner))
	offerSequence := uint32String(tx.OfferSequence)
	escrowNode := findLedgerNode(meta, "escrow", func(node *xrp.LedgerNode) bool {
		fields := ledgerNodeFields(node)
		if fields == nil {
			return false
		}
		if owner != "" && !strings.EqualFold(normalizeXRPAddress(fields.Account), owner) {
			return false
		}
		return true
	})
	fields := ledgerNodeFields(escrowNode)
	if fields == nil {
		return types.Transaction{}, false
	}

	if owner == "" {
		owner = normalizeXRPAddress(fields.Account)
	}
	to := normalizeXRPAddress(fields.Destination)
	if owner == "" || to == "" {
		return types.Transaction{}, false
	}
	if !x.isMonitoredTransfer(owner, to) {
		return types.Transaction{}, false
	}

	amount, txType, assetAddress, ok := parseXRPAmount(fields.Amount)
	if !ok || txType != constant.TxTypeNativeTransfer || assetAddress != "" {
		return types.Transaction{}, false
	}

	result := x.newBaseTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	result.FromAddress = owner
	result.ToAddress = to
	result.Amount = amount
	result.Type = constant.TxTypeNativeTransfer
	setMetadataString(&result, types.MetadataKeySubtype, "escrow_finish")
	setMetadataString(&result, types.MetadataKeyEscrowOwner, owner)
	setMetadataString(&result, types.MetadataKeyEscrowSequence, offerSequence)
	setMetadataString(&result, types.MetadataKeyDestinationTag, formatDestinationTag(fields.DestinationTag))
	return result, true
}

func (x *XRPIndexer) convertClawbackTransaction(
	tx xrp.Transaction,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) (types.Transaction, bool) {
	if strings.TrimSpace(tx.Holder) != "" {
		return types.Transaction{}, false
	}

	parsed, ok := parseXRPIssuedAmount(tx.Amount)
	if !ok {
		return types.Transaction{}, false
	}
	holder := normalizeXRPAddress(parsed.Issuer)
	if holder == "" || !x.isMonitoredAddress(holder) {
		return types.Transaction{}, false
	}

	result := x.newBaseTransaction(tx, ledgerIndex, blockHash, txIndex, ledgerTimestamp)
	result.FromAddress = holder
	result.ToAddress = xrpBurnAddress
	result.AssetAddress = formatXRPIssuedCurrency(tx.Account, parsed.Currency)
	result.Amount = strings.TrimSpace(parsed.Value)
	result.Type = constant.TxTypeTokenTransfer
	setMetadataString(&result, types.MetadataKeySubtype, "clawback")
	return result, true
}

func (x *XRPIndexer) newBaseTransaction(
	tx xrp.Transaction,
	ledgerIndex uint64,
	blockHash string,
	txIndex int,
	ledgerTimestamp uint64,
) types.Transaction {
	timestamp := ledgerTimestamp
	if tx.Date != nil && *tx.Date > 0 {
		timestamp = xrpLedgerTime(*tx.Date)
	}

	return types.Transaction{
		TxHash:        strings.TrimSpace(tx.Hash),
		NetworkId:     x.config.NetworkId,
		BlockNumber:   ledgerIndex,
		BlockHash:     blockHash,
		TransferIndex: fmt.Sprintf("%d", txIndex),
		TxFee:         dropsToXRP(strings.TrimSpace(tx.Fee)),
		Timestamp:     timestamp,
		Confirmations: 1,
		Status:        types.StatusConfirmed,
	}
}

func (x *XRPIndexer) isMonitoredAddress(address string) bool {
	if x.pubkeyStore == nil {
		return true
	}
	address = normalizeXRPAddress(address)
	return address != "" && x.pubkeyStore.Exist(enum.NetworkTypeXRP, address)
}

func accountDeleteAmount(meta *xrp.Meta, destination string) (string, bool) {
	destination = normalizeXRPAddress(destination)
	if meta == nil || destination == "" {
		return "", false
	}
	for _, affected := range meta.AffectedNodes {
		for _, node := range []*xrp.LedgerNode{affected.ModifiedNode, affected.CreatedNode} {
			if node == nil || !strings.EqualFold(strings.TrimSpace(node.LedgerEntryType), "accountroot") {
				continue
			}
			fields := ledgerNodeFields(node)
			if fields == nil || !strings.EqualFold(normalizeXRPAddress(fields.Account), destination) {
				continue
			}
			finalBalance := xrpNumericString(fields.Balance)
			previousBalance := ""
			if node.PreviousFields != nil {
				previousBalance = xrpNumericString(node.PreviousFields.Balance)
			}
			if finalBalance == "" || previousBalance == "" {
				continue
			}
			delta := decimal.RequireFromString(finalBalance).Sub(decimal.RequireFromString(previousBalance))
			if delta.Cmp(decimal.Zero) <= 0 {
				continue
			}
			return delta.Div(decimal.NewFromInt(1_000_000)).String(), true
		}
	}
	return "", false
}

func findLedgerNode(meta *xrp.Meta, entryType string, match func(node *xrp.LedgerNode) bool) *xrp.LedgerNode {
	if meta == nil {
		return nil
	}
	for _, affected := range meta.AffectedNodes {
		for _, node := range []*xrp.LedgerNode{affected.DeletedNode, affected.ModifiedNode, affected.CreatedNode} {
			if node == nil || !strings.EqualFold(strings.TrimSpace(node.LedgerEntryType), entryType) {
				continue
			}
			if match == nil || match(node) {
				return node
			}
		}
	}
	return nil
}

func ledgerNodeFields(node *xrp.LedgerNode) *xrp.LedgerFields {
	if node == nil {
		return nil
	}
	switch {
	case node.FinalFields != nil:
		return node.FinalFields
	case node.NewFields != nil:
		return node.NewFields
	default:
		return node.PreviousFields
	}
}

func parseXRPAmount(raw any) (amount string, txType constant.TxType, assetAddress string, ok bool) {
	switch value := raw.(type) {
	case string:
		return dropsToXRP(value).String(), constant.TxTypeNativeTransfer, "", true
	case map[string]any:
		var parsed xrp.IssuedCurrencyAmount
		data, err := json.Marshal(value)
		if err != nil {
			return "", "", "", false
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			return "", "", "", false
		}
		if strings.TrimSpace(parsed.Issuer) == "" || strings.TrimSpace(parsed.Currency) == "" || strings.TrimSpace(parsed.Value) == "" {
			return "", "", "", false
		}
		return strings.TrimSpace(parsed.Value), constant.TxTypeTokenTransfer, formatXRPIssuedCurrency(parsed.Issuer, parsed.Currency), true
	case xrp.IssuedCurrencyAmount:
		if strings.TrimSpace(value.Issuer) == "" || strings.TrimSpace(value.Currency) == "" || strings.TrimSpace(value.Value) == "" {
			return "", "", "", false
		}
		return strings.TrimSpace(value.Value), constant.TxTypeTokenTransfer, formatXRPIssuedCurrency(value.Issuer, value.Currency), true
	default:
		return "", "", "", false
	}
}

func xrpNumericString(raw any) string {
	switch value := raw.(type) {
	case string:
		return strings.TrimSpace(value)
	case json.Number:
		return strings.TrimSpace(value.String())
	default:
		return ""
	}
}

func parseXRPIssuedAmount(raw any) (xrp.IssuedCurrencyAmount, bool) {
	switch value := raw.(type) {
	case map[string]any:
		var parsed xrp.IssuedCurrencyAmount
		data, err := json.Marshal(value)
		if err != nil {
			return xrp.IssuedCurrencyAmount{}, false
		}
		if err := json.Unmarshal(data, &parsed); err != nil {
			return xrp.IssuedCurrencyAmount{}, false
		}
		if strings.TrimSpace(parsed.Issuer) == "" || strings.TrimSpace(parsed.Currency) == "" || strings.TrimSpace(parsed.Value) == "" {
			return xrp.IssuedCurrencyAmount{}, false
		}
		return parsed, true
	case xrp.IssuedCurrencyAmount:
		if strings.TrimSpace(value.Issuer) == "" || strings.TrimSpace(value.Currency) == "" || strings.TrimSpace(value.Value) == "" {
			return xrp.IssuedCurrencyAmount{}, false
		}
		return value, true
	default:
		return xrp.IssuedCurrencyAmount{}, false
	}
}

func formatXRPIssuedCurrency(issuer string, currency string) string {
	return normalizeXRPAddress(issuer) + ":" + strings.ToUpper(strings.TrimSpace(currency))
}

func dropsToXRP(drops string) decimal.Decimal {
	if strings.TrimSpace(drops) == "" {
		return decimal.Zero
	}
	return decimal.RequireFromString(strings.TrimSpace(drops)).Div(decimal.NewFromInt(1_000_000))
}

func xrpLedgerTime(v int64) uint64 {
	if v <= 0 {
		return 0
	}
	return uint64(v + xrpEpochOffset)
}

func normalizeXRPAddress(address string) string {
	return strings.TrimSpace(address)
}

func setMetadataString(tx *types.Transaction, key string, value string) {
	if strings.TrimSpace(value) == "" {
		return
	}
	tx.SetMetadata(key, strings.TrimSpace(value))
}

func formatDestinationTag(tag *uint32) string {
	if tag == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*tag), 10)
}

func uint32String(v *uint32) string {
	if v == nil {
		return ""
	}
	return strconv.FormatUint(uint64(*v), 10)
}

func firstBlockError(results []BlockResult) error {
	for _, result := range results {
		if result.Error != nil {
			return fmt.Errorf("block %d: %s", result.Number, result.Error.Message)
		}
	}
	return nil
}
