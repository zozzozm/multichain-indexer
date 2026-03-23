package xrp

import "encoding/json"

type Ledger struct {
	LedgerHash   string        `json:"ledger_hash"`
	ParentHash   string        `json:"parent_hash"`
	LedgerIndex  json.Number   `json:"ledger_index"`
	CloseTime    int64         `json:"close_time"`
	Transactions []Transaction `json:"transactions"`
}

type Transaction struct {
	Hash            string      `json:"hash"`
	Account         string      `json:"Account"`
	Destination     string      `json:"Destination"`
	DestinationTag  *uint32     `json:"DestinationTag,omitempty"`
	CheckID         string      `json:"CheckID,omitempty"`
	Owner           string      `json:"Owner,omitempty"`
	OfferSequence   *uint32     `json:"OfferSequence,omitempty"`
	Holder          string      `json:"Holder,omitempty"`
	TransactionType string      `json:"TransactionType"`
	Fee             string      `json:"Fee"`
	Date            *int64      `json:"date,omitempty"`
	LedgerIndex     json.Number `json:"ledger_index"`
	Amount          any         `json:"Amount"`
	Meta            *Meta       `json:"meta,omitempty"`
	MetaData        *Meta       `json:"metaData,omitempty"`
}

type Meta struct {
	TransactionResult string `json:"TransactionResult"`
	DeliveredAmount   any    `json:"delivered_amount"`
	AffectedNodes     []Node `json:"AffectedNodes"`
}

type IssuedCurrencyAmount struct {
	Currency string `json:"currency"`
	Issuer   string `json:"issuer"`
	Value    string `json:"value"`
}

type Node struct {
	CreatedNode  *LedgerNode `json:"CreatedNode,omitempty"`
	ModifiedNode *LedgerNode `json:"ModifiedNode,omitempty"`
	DeletedNode  *LedgerNode `json:"DeletedNode,omitempty"`
}

type LedgerNode struct {
	LedgerEntryType string        `json:"LedgerEntryType"`
	LedgerIndex     string        `json:"LedgerIndex"`
	FinalFields     *LedgerFields `json:"FinalFields,omitempty"`
	NewFields       *LedgerFields `json:"NewFields,omitempty"`
	PreviousFields  *LedgerFields `json:"PreviousFields,omitempty"`
}

type LedgerFields struct {
	Account        string  `json:"Account,omitempty"`
	Balance        any     `json:"Balance,omitempty"`
	Destination    string  `json:"Destination,omitempty"`
	DestinationTag *uint32 `json:"DestinationTag,omitempty"`
	Amount         any     `json:"Amount,omitempty"`
}

type ledgerResponse struct {
	Ledger       *Ledger     `json:"ledger"`
	LedgerIndex  json.Number `json:"ledger_index"`
	Validated    bool        `json:"validated"`
	Error        string      `json:"error"`
	ErrorMessage string      `json:"error_message"`
}
