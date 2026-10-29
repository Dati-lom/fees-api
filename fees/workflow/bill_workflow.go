// Package workflow holds the Temporal workflow that owns a bill's lifecycle,
// one workflow execution per bill (the "entity workflow" pattern).
//
// The workflow is the single writer for its bill: every change goes through
// it and is applied one at a time, so add-item and close can't race. The
// database is a durable copy kept in sync by activities and used for reads.
package workflow

import (
	"errors"

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
	// SignalClose closes the bill before its period ends.
	SignalClose = "close"
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

// acts is only used to name activity methods for ExecuteActivity.
var acts *Activities

type billWorkflow struct {
	bill domain.Bill
	// persisted is set once the bill row exists, so line items can be stored.
	persisted bool
	// inflight counts accepted line items whose DB insert hasn't finished.
	// Closing waits for it to reach zero so the DB sum matches the total.
	inflight      int
	chargeAttempt int
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

	w.waitForPeriodEndOrClose(ctx)

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
	return workflow.SetUpdateHandlerWithOptions(ctx, UpdateAddLineItem, w.addLineItem,
		workflow.UpdateHandlerOptions{Validator: w.validateLineItem})
}

// validateLineItem runs before the update is accepted. A rejection here is
// not written to workflow history, so invalid requests cost nothing.
// It must not change state.
func (w *billWorkflow) validateLineItem(ctx workflow.Context, in AddLineItemInput) error {
	if _, ok := w.findItem(in.IdempotencyKey); ok {
		return nil // retry of an item already accepted: always succeeds
	}
	return rejection(w.bill.ValidateLineItem(w.newLineItem(ctx, in)))
}

// addLineItem adds the item to the bill and stores it, replying only once
// the item is in the database.
func (w *billWorkflow) addLineItem(ctx workflow.Context, in AddLineItemInput) (domain.LineItem, error) {
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

// waitForPeriodEndOrClose blocks until the period ends or a close signal
// arrives, whichever happens first.
func (w *billWorkflow) waitForPeriodEndOrClose(ctx workflow.Context) {
	timerCtx, cancelTimer := workflow.WithCancel(ctx)
	defer cancelTimer()

	sel := workflow.NewSelector(ctx)
	sel.AddReceive(workflow.GetSignalChannel(ctx, SignalClose), func(c workflow.ReceiveChannel, _ bool) {
		c.Receive(ctx, nil)
	})
	if d := w.bill.PeriodEnd.Sub(workflow.Now(ctx)); d > 0 {
		sel.AddFuture(workflow.NewTimer(timerCtx, d), func(workflow.Future) {})
	} else {
		return // period already over
	}
	sel.Select(ctx)
}

// close freezes the total, charges it and records the result. It reports
// whether the bill ended CLOSED (true) or CHARGE_FAILED (false).
func (w *billWorkflow) close(ctx workflow.Context) (bool, error) {
	// Freeze first: from here on the validator rejects new items.
	if err := w.bill.StartClosing(); err != nil {
		return false, err
	}
	// Then wait for items accepted just before the freeze to be stored.
	if err := workflow.Await(ctx, func() bool { return w.inflight == 0 }); err != nil {
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

// charge runs ChargeBill. Any error means the charge didn't happen: either it
// was declined or every retry failed.
func (w *billWorkflow) charge(ctx workflow.Context, total money.Money) error {
	w.chargeAttempt++
	chargeCtx := workflow.WithActivityOptions(ctx, chargeActivityOptions)
	var res ChargeBillResult
	err := workflow.ExecuteActivity(chargeCtx, acts.ChargeBill, ChargeBillInput{
		BillID:    w.bill.ID,
		AccountID: w.bill.AccountID,
		Amount:    total,
		Attempt:   w.chargeAttempt,
	}).Get(ctx, &res)

	logger := workflow.GetLogger(ctx)
	if err != nil {
		logger.Warn("charge failed", "billID", w.bill.ID, "attempt", w.chargeAttempt, "error", err)
		return err
	}
	logger.Info("charge succeeded", "billID", w.bill.ID, "chargeID", res.ChargeID)
	return nil
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
	default:
		return err
	}
	return temporal.NewNonRetryableApplicationError(err.Error(), errType, err)
}
