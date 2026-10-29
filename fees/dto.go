package fees

import (
	"strings"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.dev/beta/errs"
	"encore.dev/types/uuid"
)

// Request DTOs only check that the request is well-formed (required fields
// present). Business rules (supported currency, valid period, bill is open)
// are enforced by the domain package so they apply no matter who calls it.

// CreateBillRequest is the body of POST /bills.
type CreateBillRequest struct {
	AccountID   string             `json:"account_id"`
	Currency    money.CurrencyCode `json:"currency"`
	PeriodStart time.Time          `json:"period_start"`
	PeriodEnd   time.Time          `json:"period_end"`
}

// Validate is called by Encore before the handler runs.
func (r *CreateBillRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.AccountID) == "":
		return errs.B().Code(errs.InvalidArgument).Msg("account_id is required").Err()
	case r.Currency == "":
		return errs.B().Code(errs.InvalidArgument).Msg("currency is required").Err()
	case r.PeriodStart.IsZero() || r.PeriodEnd.IsZero():
		return errs.B().Code(errs.InvalidArgument).Msg("period_start and period_end are required").Err()
	}
	return nil
}

func (r *CreateBillRequest) toParams(id uuid.UUID, now time.Time) domain.NewBillParams {
	return domain.NewBillParams{
		ID:          id,
		AccountID:   strings.TrimSpace(r.AccountID),
		Currency:    r.Currency,
		PeriodStart: r.PeriodStart,
		PeriodEnd:   r.PeriodEnd,
		Now:         now,
	}
}

// AddLineItemRequest is the body of POST /bills/:id/line-items.
type AddLineItemRequest struct {
	// IdempotencyKey lets clients safely retry: the same key is never added twice.
	IdempotencyKey string `header:"Idempotency-Key"`
	Description    string `json:"description"`
	// Amount is in minor units; negative for a refund. Currency must match the bill.
	Amount money.Money `json:"amount"`
}

func (r *AddLineItemRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.IdempotencyKey) == "":
		return errs.B().Code(errs.InvalidArgument).Msg("Idempotency-Key header is required").Err()
	case strings.TrimSpace(r.Description) == "":
		return errs.B().Code(errs.InvalidArgument).Msg("description is required").Err()
	}
	return nil
}

func (r *AddLineItemRequest) toLineItem(id, billID uuid.UUID, now time.Time) domain.LineItem {
	return domain.LineItem{
		ID:             id,
		BillID:         billID,
		Description:    strings.TrimSpace(r.Description),
		Amount:         r.Amount,
		IdempotencyKey: r.IdempotencyKey,
		CreatedAt:      now,
	}
}

// BillResponse is returned by every endpoint that returns a bill.
type BillResponse struct {
	ID          uuid.UUID          `json:"id"`
	AccountID   string             `json:"account_id"`
	Currency    money.CurrencyCode `json:"currency"`
	Status      domain.Status      `json:"status"`
	PeriodStart time.Time          `json:"period_start"`
	PeriodEnd   time.Time          `json:"period_end"`
	Total       *money.Money       `json:"total"` // null until the bill starts closing
	CreatedAt   time.Time          `json:"created_at"`
	ClosedAt    *time.Time         `json:"closed_at"`
	LineItems   []LineItemResponse `json:"line_items"`
}

type LineItemResponse struct {
	ID          uuid.UUID   `json:"id"`
	Description string      `json:"description"`
	Amount      money.Money `json:"amount"`
	CreatedAt   time.Time   `json:"created_at"`
}

func newBillResponse(b domain.Bill) *BillResponse {
	items := make([]LineItemResponse, 0, len(b.LineItems))
	for _, li := range b.LineItems {
		items = append(items, LineItemResponse{
			ID:          li.ID,
			Description: li.Description,
			Amount:      li.Amount,
			CreatedAt:   li.CreatedAt,
		})
	}
	return &BillResponse{
		ID:          b.ID,
		AccountID:   b.AccountID,
		Currency:    b.Currency,
		Status:      b.Status,
		PeriodStart: b.PeriodStart,
		PeriodEnd:   b.PeriodEnd,
		Total:       b.Total,
		CreatedAt:   b.CreatedAt,
		ClosedAt:    b.ClosedAt,
		LineItems:   items,
	}
}
