package fees

import (
	"slices"
	"strings"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	billworkflow "encore.app/fees/workflow"
	"encore.dev/beta/errs"
	"encore.dev/types/uuid"
)

// Request DTOs only check that the request is well-formed (required fields
// present). Business rules (supported currency, valid period, bill is open)
// are enforced by the domain package so they apply no matter who calls it.

// CreateBillRequest is the body of POST /bills.
type CreateBillRequest struct {
	// IdempotencyKey makes retried creates return the same bill instead of
	// opening a second one.
	IdempotencyKey string             `header:"Idempotency-Key"`
	AccountID      string             `json:"account_id"`
	Currency       money.CurrencyCode `json:"currency"`
	PeriodStart    time.Time          `json:"period_start"`
	PeriodEnd      time.Time          `json:"period_end"`
}

// Validate is called by Encore before the handler runs.
func (r *CreateBillRequest) Validate() error {
	switch {
	case strings.TrimSpace(r.IdempotencyKey) == "":
		return errs.B().Code(errs.InvalidArgument).Msg("Idempotency-Key header is required").Err()
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

func (r *AddLineItemRequest) toInput(id uuid.UUID) billworkflow.AddLineItemInput {
	return billworkflow.AddLineItemInput{
		ID:             id,
		Description:    strings.TrimSpace(r.Description),
		Amount:         r.Amount,
		IdempotencyKey: r.IdempotencyKey,
	}
}

// ListBillsRequest is the query string of GET /bills.
type ListBillsRequest struct {
	AccountID string `query:"account_id"`
	Status    string `query:"status"`
	Limit     int    `query:"limit"`
}

var listableStatuses = []domain.Status{
	domain.StatusOpen, domain.StatusClosing, domain.StatusClosed, domain.StatusFailed,
}

func (r *ListBillsRequest) Validate() error {
	switch {
	case r.Status != "" && !slices.Contains(listableStatuses, domain.Status(r.Status)):
		return errs.B().Code(errs.InvalidArgument).Msgf("unknown status %q", r.Status).Err()
	case r.Limit < 0:
		return errs.B().Code(errs.InvalidArgument).Msg("limit must not be negative").Err()
	}
	return nil
}

type ListBillsResponse struct {
	// Bills are returned without line items; use GET /bills/:id for those.
	Bills []*BillResponse `json:"bills"`
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
		items = append(items, *newLineItemResponse(li))
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

func newLineItemResponse(li domain.LineItem) *LineItemResponse {
	return &LineItemResponse{
		ID:          li.ID,
		Description: li.Description,
		Amount:      li.Amount,
		CreatedAt:   li.CreatedAt,
	}
}

type ListCurrenciesResponse struct {
	Currencies []CurrencyResponse `json:"currencies"`
}

type CurrencyResponse struct {
	Code money.CurrencyCode `json:"code"`
	Name string             `json:"name"`
	// MinorUnits is the number of decimal places amounts are expressed in:
	// 2 means an amount of 1250 is 12.50.
	MinorUnits int `json:"minor_units"`
}
