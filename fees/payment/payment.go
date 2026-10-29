// Package payment is the boundary to the payment provider that actually moves
// money. Only a mock exists for now; a real provider (card processor, core
// banking ledger) would implement the same Charger interface.
package payment

import (
	"context"
	"errors"

	"encore.app/fees/money"
	"encore.dev/types/uuid"
)

// ErrDeclined means the provider refused the charge (e.g. insufficient funds).
// It is permanent: retrying the same charge won't succeed.
var ErrDeclined = errors.New("payment: charge declined")

type ChargeRequest struct {
	BillID    uuid.UUID
	AccountID string
	Amount    money.Money
	// IdempotencyKey makes the provider return the original result instead of
	// charging twice when the same request is retried.
	IdempotencyKey string
}

type ChargeResult struct {
	// ChargeID is the provider's reference for the charge.
	ChargeID string
}

type Charger interface {
	Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error)
}
