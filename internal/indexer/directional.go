package indexer

import (
	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/fystack/multichain-indexer/pkg/common/types"
)

const (
	metadataKeySourceAmount = "source_amount"
	metadataKeySourceAsset  = "source_asset_address"
	metadataKeySourceTxType = "source_tx_type"
)

func normalizeDirectionalMetadata(tx types.Transaction, direction string) types.Transaction {
	if direction != types.DirectionOut {
		clearDirectionalMetadata(&tx)
		return tx
	}

	sourceType := tx.GetMetadataString(metadataKeySourceTxType)
	if sourceType != "" {
		tx.Type = constant.TxType(sourceType)
		if tx.Type == constant.TxTypeNativeTransfer {
			tx.AssetAddress = ""
		}
	}

	if sourceAmount := tx.GetMetadataString(metadataKeySourceAmount); sourceAmount != "" {
		tx.Amount = sourceAmount
	}

	switch tx.GetMetadataString(metadataKeySourceTxType) {
	case string(constant.TxTypeNativeTransfer):
		tx.AssetAddress = ""
	case string(constant.TxTypeTokenTransfer):
		tx.AssetAddress = tx.GetMetadataString(metadataKeySourceAsset)
	}

	clearDirectionalMetadata(&tx)
	return tx
}

func clearDirectionalMetadata(tx *types.Transaction) {
	if tx == nil || tx.Metadata == nil {
		return
	}

	delete(tx.Metadata, metadataKeySourceAmount)
	delete(tx.Metadata, metadataKeySourceAsset)
	delete(tx.Metadata, metadataKeySourceTxType)
	if len(tx.Metadata) == 0 {
		tx.Metadata = nil
	}
}
