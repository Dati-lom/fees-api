package workflow

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.app/fees/payment"
	"encore.app/fees/store"
	"encore.dev/types/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// Activities are the workflow's side effects: database writes and the charge.
// Temporal retries a failed activity, so each one must be safe to run more
// than once; the store and the charger are both idempotent.
//
// Errors are split in two:
//   - permanent (bill not open, declined, ...) are returned as non-retryable
//     ApplicationErrors with one of the ErrType* types, so the workflow can
//     react to them;
//   - everything else (DB down, provider timeout) is returned as is and
//     Temporal retries it with backoff.
type Activities struct {
	Store   *store.Store
	Charger payment.Charger
}

// Error types of non-retryable activity errors. The workflow matches on these
// with temporal.ApplicationError.Type().
const (
	ErrTypeNotFound          = "NotFound"
	ErrTypeBillNotOpen       = "BillNotOpen"
	ErrTypeInvalidTransition = "InvalidTransition"
	ErrTypeTotalMismatch     = "TotalMismatch"
	ErrTypeChargeDeclined    = "ChargeDeclined"
	ErrTypeInvalidCharge     = "InvalidCharge"
)

// Activity options used by the workflow.
var (
	// dbActivityOptions: DB writes are cheap; keep retrying transient errors
	// (no attempt limit) so a DB outage delays the bill instead of failing it.
	dbActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 10 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    time.Minute,
		},
	}

	// chargeActivityOptions: a bounded number of attempts; when they run out
	// the bill goes to CHARGE_FAILED for someone to look at.
	chargeActivityOptions = workflow.ActivityOptions{
		StartToCloseTimeout: 30 * time.Second,
		RetryPolicy: &temporal.RetryPolicy{
			InitialInterval:    time.Second,
			BackoffCoefficient: 2,
			MaximumInterval:    30 * time.Second,
			MaximumAttempts:    5,
		},
	}
)

func (a *Activities) InsertBill(ctx context.Context, bill domain.Bill) error {
	return classify(a.Store.InsertBill(ctx, bill))
}

func (a *Activities) InsertLineItem(ctx context.Context, item domain.LineItem) error {
	return classify(a.Store.InsertLineItem(ctx, item))
}

type StartClosingInput struct {
	BillID uuid.UUID
	Total  money.Money
}

func (a *Activities) StartClosing(ctx context.Context, in StartClosingInput) error {
	return classify(a.Store.StartClosing(ctx, in.BillID, in.Total))
}

type MarkClosedInput struct {
	BillID   uuid.UUID
	ClosedAt time.Time
}

func (a *Activities) MarkClosed(ctx context.Context, in MarkClosedInput) error {
	return classify(a.Store.MarkClosed(ctx, in.BillID, in.ClosedAt))
}

type MarkChargeFailedInput struct {
	BillID uuid.UUID
}

func (a *Activities) MarkChargeFailed(ctx context.Context, in MarkChargeFailedInput) error {
	return classify(a.Store.MarkChargeFailed(ctx, in.BillID))
}

type ChargeBillInput struct {
	BillID    uuid.UUID
	AccountID string
	Amount    money.Money
	// IdempotencyKey is chosen by the workflow (see chargeIdempotencyKey) and
	// passed to the provider unchanged, so every retry of the same charge,
	// by Temporal or after CHARGE_FAILED, is deduplicated by the provider.
	IdempotencyKey string
}

type ChargeBillResult struct {
	ChargeID string
}

// ChargeBill charges the bill's frozen total. Only positive amounts can be
// charged; the workflow decides what to do with zero or negative totals.
func (a *Activities) ChargeBill(ctx context.Context, in ChargeBillInput) (ChargeBillResult, error) {
	if !in.Amount.IsPositive() {
		return ChargeBillResult{}, temporal.NewNonRetryableApplicationError(
			fmt.Sprintf("cannot charge non-positive amount %s", in.Amount), ErrTypeInvalidCharge, nil)
	}

	res, err := a.Charger.Charge(ctx, payment.ChargeRequest{
		BillID:         in.BillID,
		AccountID:      in.AccountID,
		Amount:         in.Amount,
		IdempotencyKey: in.IdempotencyKey,
	})
	if errors.Is(err, payment.ErrDeclined) {
		return ChargeBillResult{}, temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeChargeDeclined, err)
	}
	if err != nil {
		return ChargeBillResult{}, err
	}
	return ChargeBillResult{ChargeID: res.ChargeID}, nil
}

// classify turns permanent store errors into non-retryable activity errors and
// leaves the rest retryable.
func classify(err error) error {
	if err == nil {
		return nil
	}
	errType := ""
	switch {
	case errors.Is(err, store.ErrNotFound):
		errType = ErrTypeNotFound
	case errors.Is(err, store.ErrBillNotOpen):
		errType = ErrTypeBillNotOpen
	case errors.Is(err, store.ErrInvalidTransition):
		errType = ErrTypeInvalidTransition
	case errors.Is(err, store.ErrTotalMismatch):
		errType = ErrTypeTotalMismatch
	default:
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
}
