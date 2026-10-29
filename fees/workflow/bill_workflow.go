// Package workflow holds the Temporal workflow that owns a bill's lifecycle,
// one workflow execution per bill (the "entity workflow" pattern).
//
// The workflow is the single writer for its bill: every change goes through
// it and is applied one at a time, so add-item and close can't race. The
// database is a durable copy kept in sync by activities and used for reads.
package workflow

import (
	"errors"
	"fmt"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.dev/types/uuid"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the Temporal task queue the fees worker polls.
const TaskQueue = "fees"

// Names of the messages the API sends to a bill workflow.
const (
	// UpdateAddLineItem adds a line item and returns it, or rejects it.
	UpdateAddLineItem = "addLineItem"
	// UpdateClose closes the bill before its period ends and returns it once
	// its total is frozen. Charging continues in the background.
	UpdateClose = "close"
	// SignalRetryCharge retries closing a bill that is CHARGE_FAILED.
	SignalRetryCharge = "retryCharge"
	// QueryGetBill returns the workflow's current view of the bill.
	QueryGetBill = "getBill"
)

// Error types for rejected line items (in addition to ErrTypeBillNotOpen).
const (
	ErrTypeCurrencyMismatch = "CurrencyMismatch"
	ErrTypeInvalidAmount    = "InvalidAmount"
	ErrTypeInvalidBill      = "InvalidBill"
	ErrTypeKeyReused        = "IdempotencyKeyReused"
)

// WorkflowID returns the workflow ID for a bill. Using the bill ID makes
// starting the same bill twice fail, so bill creation is idempotent.
func WorkflowID(billID uuid.UUID) string {
	return "bill-" + billID.String()
}

// AddLineItemInput is the argument of UpdateAddLineItem. The ID is generated
// by the caller because workflow code must be deterministic.
type AddLineItemInput struct {
	ID             uuid.UUID
	Description    string
	Amount         money.Money
	IdempotencyKey string
}

// Matches reports whether item is what in describes, i.e. whether in is a
// genuine retry of the request that added item. ID is ignored: the API
// generates a new one on every attempt.
func (in AddLineItemInput) Matches(item domain.LineItem) bool {
	return in.Description == item.Description && in.Amount == item.Amount
}

// acts is only used to name activity methods for ExecuteActivity.
var acts *Activities

type billWorkflow struct {
	bill domain.Bill
	// persisted is set once the bill row exists, so line items can be stored.
	persisted bool
	// inflight counts accepted line items whose DB insert hasn't finished.
	// Closing waits for it to reach zero so the DB sum matches the total.
	inflight int
	// closeRequested is set by UpdateClose; the main loop acts on it.
	closeRequested bool
	// frozen is set once the CLOSING status and total are stored, i.e. the
	// amount to charge is fixed for good.
	frozen bool
	// declinedCharges counts charges the provider declined; it selects the
	// idempotency key of the next charge (see chargeIdempotencyKey).
	declinedCharges int
}

// BillWorkflow runs for the whole life of one bill:
func BillWorkflow(ctx workflow.Context, params domain.NewBillParams) error {
	bill, err := domain.NewBill(params)
	if err != nil {
		return temporal.NewNonRetryableApplicationError(err.Error(), ErrTypeInvalidBill, err)
	}
	w := &billWorkflow{bill: bill}

	// Handlers must be registered before anything blocks, otherwise updates
	// that arrive early are rejected as unknown.
	if err := w.registerHandlers(ctx); err != nil {
		return err
	}

	dbCtx := workflow.WithActivityOptions(ctx, dbActivityOptions)
	if err := workflow.ExecuteActivity(dbCtx, acts.InsertBill, w.bill).Get(ctx, nil); err != nil {
		return err
	}
	w.persisted = true

	if err := w.waitForPeriodEndOrClose(ctx); err != nil {
		return err
	}

	for {
		closed, err := w.close(ctx)
		if err != nil {
			return err
		}
		if closed {
			break
		}
		// CHARGE_FAILED: stay alive until someone asks to retry.
		workflow.GetSignalChannel(ctx, SignalRetryCharge).Receive(ctx, nil)
	}

	// Let update handlers that are still running reply before completing.
	return workflow.Await(ctx, func() bool { return workflow.AllHandlersFinished(ctx) })
}

func (w *billWorkflow) registerHandlers(ctx workflow.Context) error {
	err := workflow.SetQueryHandler(ctx, QueryGetBill, func() (domain.Bill, error) {
		return w.bill, nil
	})
	if err != nil {
		return err
	}
	err = workflow.SetUpdateHandlerWithOptions(ctx, UpdateAddLineItem, w.addLineItem,
		workflow.UpdateHandlerOptions{Validator: w.validateLineItem})
	if err != nil {
		return err
	}
	return workflow.SetUpdateHandler(ctx, UpdateClose, w.requestClose)
}

// requestClose asks the main loop to close the bill and replies once the
// total is frozen and stored, with the total and the line items it covers.
// Closing a bill that is already closing or closed just returns it.
func (w *billWorkflow) requestClose(ctx workflow.Context) (domain.Bill, error) {
	w.closeRequested = true
	if err := workflow.Await(ctx, func() bool { return w.frozen }); err != nil {
		return domain.Bill{}, err
	}
	return w.bill, nil
}

// validateLineItem runs before the update is accepted. A rejection here is
// not written to workflow history, so invalid requests cost nothing.
// It must not change state.
func (w *billWorkflow) validateLineItem(ctx workflow.Context, in AddLineItemInput) error {
	if existing, ok := w.findItem(in.IdempotencyKey); ok {
		// A retry of an item already accepted succeeds, even once the bill
		// is closed, but only if it is the same request.
		if !in.Matches(existing) {
			return rejection(domain.ErrIdempotencyKeyReused)
		}
		return nil
	}
	return rejection(w.bill.ValidateLineItem(w.newLineItem(ctx, in)))
}

// addLineItem adds the item to the bill and stores it, replying only once
// the item is in the database.
func (w *billWorkflow) addLineItem(ctx workflow.Context, in AddLineItemInput) (domain.LineItem, error) {
	// The validator has already rejected a reused key with a different
	// request, and nothing can change in between: validator and handler run
	// in the same workflow task.
	if existing, ok := w.findItem(in.IdempotencyKey); ok {
		return existing, nil
	}

	item := w.newLineItem(ctx, in)
	// Added to the in-memory bill before awaiting anything, so a close that
	// starts while the insert runs still includes this item in the total.
	if err := w.bill.AddLineItem(item); err != nil {
		return domain.LineItem{}, rejection(err)
	}
	w.inflight++
	defer func() { w.inflight-- }()

	if err := workflow.Await(ctx, func() bool { return w.persisted }); err != nil {
		w.removeItem(item.IdempotencyKey)
		return domain.LineItem{}, err
	}
	dbCtx := workflow.WithActivityOptions(ctx, dbActivityOptions)
	if err := workflow.ExecuteActivity(dbCtx, acts.InsertLineItem, item).Get(ctx, nil); err != nil {
		// Only permanent errors get here (DB activities retry transient
		// ones forever), so the item is dropped and the caller told.
		w.removeItem(item.IdempotencyKey)
		return domain.LineItem{}, err
	}
	return item, nil
}

// waitForPeriodEndOrClose blocks until the period ends or a close is
// requested, whichever happens first.
func (w *billWorkflow) waitForPeriodEndOrClose(ctx workflow.Context) error {
	d := w.bill.PeriodEnd.Sub(workflow.Now(ctx))
	if d <= 0 {
		return nil // period already over
	}
	_, err := workflow.AwaitWithTimeout(ctx, d, func() bool { return w.closeRequested })
	return err
}

// close freezes the total, charges it and records the result. It reports
// whether the bill ended CLOSED (true) or CHARGE_FAILED (false).
func (w *billWorkflow) close(ctx workflow.Context) (bool, error) {
	// Stop accepting items first: from here on the validator rejects them.
	if err := w.bill.StartClosing(); err != nil {
		return false, err
	}
	// Then wait for items accepted just before to be stored, or dropped if
	// their insert failed, and only then compute the total, so it matches
	// the items in the database.
	if err := workflow.Await(ctx, func() bool { return w.inflight == 0 }); err != nil {
		return false, err
	}
	if err := w.bill.FreezeTotal(); err != nil {
		return false, err
	}

	dbCtx := workflow.WithActivityOptions(ctx, dbActivityOptions)
	total := *w.bill.Total
	err := workflow.ExecuteActivity(dbCtx, acts.StartClosing, StartClosingInput{
		BillID: w.bill.ID,
		Total:  total,
	}).Get(ctx, nil)
	if err != nil {
		return false, err
	}
	w.frozen = true

	// Nothing to charge when refunds cancel out or exceed fees; the bill
	// closes with its (zero or negative) total recorded as a credit.
	if total.IsPositive() {
		if err := w.charge(ctx, total); err != nil {
			if err := w.bill.MarkChargeFailed(); err != nil {
				return false, err
			}
			err := workflow.ExecuteActivity(dbCtx, acts.MarkChargeFailed, MarkChargeFailedInput{
				BillID: w.bill.ID,
			}).Get(ctx, nil)
			return false, err
		}
	}

	now := workflow.Now(ctx)
	if err := w.bill.Close(now); err != nil {
		return false, err
	}
	err = workflow.ExecuteActivity(dbCtx, acts.MarkClosed, MarkClosedInput{
		BillID:   w.bill.ID,
		ClosedAt: now,
	}).Get(ctx, nil)
	return err == nil, err
}

// charge runs ChargeBill. An error means the charge was declined, or its
// outcome is unknown because every attempt failed or timed out; in the
// second case the provider may still have taken the money.
func (w *billWorkflow) charge(ctx workflow.Context, total money.Money) error {
	key := w.chargeIdempotencyKey()
	chargeCtx := workflow.WithActivityOptions(ctx, chargeActivityOptions)
	var res ChargeBillResult
	err := workflow.ExecuteActivity(chargeCtx, acts.ChargeBill, ChargeBillInput{
		BillID:         w.bill.ID,
		AccountID:      w.bill.AccountID,
		Amount:         total,
		IdempotencyKey: key,
	}).Get(ctx, &res)

	logger := workflow.GetLogger(ctx)
	if err != nil {
		var appErr *temporal.ApplicationError
		if errors.As(err, &appErr) && appErr.Type() == ErrTypeChargeDeclined {
			w.declinedCharges++
		}
		logger.Warn("charge failed", "billID", w.bill.ID, "idempotencyKey", key, "error", err)
		return err
	}
	logger.Info("charge succeeded", "billID", w.bill.ID, "chargeID", res.ChargeID)
	return nil
}

// chargeIdempotencyKey returns the key for the next charge of the bill's
// frozen total.
//
// The key only changes after a decline. A decline is a definite answer (no
// money moved), and providers replay a cached decline for a repeated key, so
// a retry needs a new one. Any other failure (timeouts, provider outage) is
// ambiguous: the charge may have gone through with the response lost. Those
// retries must reuse the key so the provider returns the original charge
// instead of taking the money a second time.
func (w *billWorkflow) chargeIdempotencyKey() string {
	return fmt.Sprintf("bill-%s-charge-%d", w.bill.ID, w.declinedCharges+1)
}

func (w *billWorkflow) newLineItem(ctx workflow.Context, in AddLineItemInput) domain.LineItem {
	return domain.LineItem{
		ID:             in.ID,
		BillID:         w.bill.ID,
		Description:    in.Description,
		Amount:         in.Amount,
		IdempotencyKey: in.IdempotencyKey,
		CreatedAt:      workflow.Now(ctx),
	}
}

func (w *billWorkflow) findItem(key string) (domain.LineItem, bool) {
	for _, li := range w.bill.LineItems {
		if li.IdempotencyKey == key {
			return li, true
		}
	}
	return domain.LineItem{}, false
}

func (w *billWorkflow) removeItem(key string) {
	for i, li := range w.bill.LineItems {
		if li.IdempotencyKey == key {
			w.bill.LineItems = append(w.bill.LineItems[:i], w.bill.LineItems[i+1:]...)
			return
		}
	}
}

// rejection turns a domain validation error into a typed application error
// the API can map to an HTTP status.
func rejection(err error) error {
	if err == nil {
		return nil
	}
	errType := ""
	switch {
	case errors.Is(err, domain.ErrBillNotOpen):
		errType = ErrTypeBillNotOpen
	case errors.Is(err, money.ErrCurrencyMismatch):
		errType = ErrTypeCurrencyMismatch
	case errors.Is(err, domain.ErrInvalidAmount):
		errType = ErrTypeInvalidAmount
	case errors.Is(err, domain.ErrIdempotencyKeyReused):
		errType = ErrTypeKeyReused
	default:
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
}
