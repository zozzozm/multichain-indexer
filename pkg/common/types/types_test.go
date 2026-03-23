package types

import (
	"testing"

	"github.com/fystack/multichain-indexer/pkg/common/constant"
	"github.com/shopspring/decimal"
	"github.com/stretchr/testify/require"
)

func TestTransactionBinaryRoundTripPreservesRoutingFields(t *testing.T) {
	t.Parallel()

	tx := Transaction{
		TxHash:        "tx-hash",
		NetworkId:     "xrp_mainnet",
		BlockNumber:   42,
		FromAddress:   "rFrom",
		ToAddress:     "rTo",
		AssetAddress:  "",
		Amount:        "10.5",
		Type:          constant.TxTypeNativeTransfer,
		TxFee:         decimal.RequireFromString("0.000012"),
		Timestamp:     1234567890,
		Confirmations: 1,
		Status:        StatusConfirmed,
		Direction:     DirectionIn,
	}
	tx.SetMetadata(MetadataKeyDestinationTag, "12345")
	tx.SetMetadata(MetadataKeyMemo, "invoice-42")
	tx.SetMetadata(MetadataKeyMemoType, "text")
	tx.SetMetadata("custom_routing_note", "internal")

	data, err := tx.MarshalBinary()
	require.NoError(t, err)

	var decoded Transaction
	require.NoError(t, decoded.UnmarshalBinary(data))
	require.Equal(t, "12345", decoded.GetMetadataString(MetadataKeyDestinationTag))
	require.Equal(t, "invoice-42", decoded.GetMetadataString(MetadataKeyMemo))
	require.Equal(t, "text", decoded.GetMetadataString(MetadataKeyMemoType))
	require.Equal(t, "internal", decoded.GetMetadataString("custom_routing_note"))
}

func TestTransactionHashIncludesRoutingFields(t *testing.T) {
	t.Parallel()

	base := Transaction{
		TxHash:      "tx-hash",
		NetworkId:   "xrp_mainnet",
		FromAddress: "rFrom",
		ToAddress:   "rTo",
		Timestamp:   1234567890,
		Direction:   DirectionIn,
	}

	withTag := base
	withTag.SetMetadata(MetadataKeyDestinationTag, "1")

	withMemo := base
	withMemo.SetMetadata(MetadataKeyMemo, "memo")
	withMemo.SetMetadata(MetadataKeyMemoType, "text")

	require.NotEqual(t, base.Hash(), withTag.Hash())
	require.NotEqual(t, base.Hash(), withMemo.Hash())
}
