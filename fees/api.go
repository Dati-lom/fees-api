package fees

import (
	"context"
	"errors"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.app/fees/store"
	billworkflow "encore.app/fees/workflow"
	"encore.dev/beta/errs"
	"encore.dev/types/uuid"
	enumspb "go.temporal.io/api/enums/v1"
	"go.temporal.io/api/serviceerror"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/temporal"
)

// The API is a thin layer: it validates input, talks to the bill's workflow
// (which owns all state changes) and reads from the database.

// workflowCallTimeout bounds calls that wait on the workflow (updates). If
// no worker answers in time the caller gets 503 and can retry with the same
// Idempotency-Key.
const workflowCallTimeout = 10 * time.Second

// billIDNamespace derives bill IDs from (account, Idempotency-Key), so a
// retried create maps to the same bill and therefore the same workflow ID.
var billIDNamespace = uuid.Must(uuid.FromString("6f2a4c1e-8d3b-4f5a-9c7e-2b1d0e3f4a5b"))

// CreateBill opens a new bill and starts its workflow. Retrying with the same
// Idempotency-Key and body returns the existing bill; reusing the key with a
// different body fails with 400.
//
//encore:api auth method=POST path=/bills
func (s *Service) CreateBill(ctx context.Context, req *CreateBillRequest) (*BillResponse, error) {
	id := uuid.NewV5(billIDNamespace, req.AccountID+"/"+req.IdempotencyKey)
	params := req.toParams(id, time.Now().UTC())
	//TODO
	//When retry when throw error connecting to Temporal

	// The currencies table decides what is supported. This is checked here,
	// not in the workflow, so the workflow never reads data that can change.
	if _, err := s.store.GetCurrency(ctx, params.Currency); err != nil {
		return nil, toAPIError(err)
	}
	// Validate here too so bad input is a 400 now, not a failed workflow.
	bill, err := domain.NewBill(params)
	if err != nil {
		return nil, toAPIError(err)
	}

	_, err = s.temporal.ExecuteWorkflow(ctx, client.StartWorkflowOptions{
		ID:        billworkflow.WorkflowID(id),
		TaskQueue: billworkflow.TaskQueue,
		// A bill's workflow ID is never reused, even after it completes,
		// so a late retry can't reopen a closed bill.
		WorkflowIDReusePolicy: enumspb.WORKFLOW_ID_REUSE_POLICY_REJECT_DUPLICATE,
		// Without this, starting a workflow that is still running silently
		// returns the existing run, and we'd answer a retry with a bill built
		// from the retry's body instead of the bill that actually exists.
		WorkflowExecutionErrorWhenAlreadyStarted: true,
	}, billworkflow.BillWorkflow, params)

	var alreadyStarted *serviceerror.WorkflowExecutionAlreadyStarted
	switch {
	case errors.As(err, &alreadyStarted):
		existing, err := s.getBill(ctx, id)
		if err != nil {
			return nil, err
		}
		if !params.Matches(existing) {
			return nil, toAPIError(domain.ErrIdempotencyKeyReused)
		}
		return newBillResponse(existing), nil
	case err != nil:
		return nil, errs.WrapCode(err, errs.Unavailable, "could not start bill workflow")
	}
	return newBillResponse(bill), nil
}

// AddLineItem adds a fee (positive amount) or refund (negative) to an open
// bill. It returns only once the item is stored, and fails with 409 if the
// bill is no longer open. A refund larger than the bill's current total fails
// with 400, so a bill never ends with a credit. Retrying with the same Idempotency-Key and body
// returns the original item; reusing the key with a different body fails
// with 400.
//
//encore:api auth method=POST path=/bills/:id/line-items
func (s *Service) AddLineItem(ctx context.Context, id uuid.UUID, req *AddLineItemRequest) (*LineItemResponse, error) {
	itemID, err := uuid.NewV4()
	if err != nil {
		return nil, errs.WrapCode(err, errs.Internal, "generate line item id")
	}

	ctx, cancel := context.WithTimeout(ctx, workflowCallTimeout)
	defer cancel()

	in := req.toInput(itemID)
	handle, err := s.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID: billworkflow.WorkflowID(id),
		// Temporal deduplicates updates by ID, so a retried request is
		// applied once.
		UpdateID:     req.IdempotencyKey,
		UpdateName:   billworkflow.UpdateAddLineItem,
		Args:         []any{in},
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err != nil {
		return s.lineItemCallError(ctx, id, in, err)
	}
	var item domain.LineItem
	if err := handle.Get(ctx, &item); err != nil {
		return s.lineItemCallError(ctx, id, in, err)
	}
	// For a repeated UpdateID Temporal returns the first update's result
	// without running the workflow's validator, so a reused key is caught
	// here rather than there.
	if !in.Matches(item) {
		return nil, toAPIError(domain.ErrIdempotencyKeyReused)
	}
	return newLineItemResponse(item), nil
}

// lineItemCallError handles a failed AddLineItem update. Temporal only
// deduplicates updates while the workflow runs, so once the bill is closed
// and its workflow has completed, a retry of an item that was added gets
// NotFound. The item is then looked up by its key in the database, so the
// retry still returns the original item (or 400 for a different body).
func (s *Service) lineItemCallError(ctx context.Context, id uuid.UUID, in billworkflow.AddLineItemInput, err error) (*LineItemResponse, error) {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		item, dbErr := s.store.GetLineItemByKey(ctx, id, in.IdempotencyKey)
		switch {
		case dbErr == nil:
			if !in.Matches(item) {
				return nil, toAPIError(domain.ErrIdempotencyKeyReused)
			}
			return newLineItemResponse(item), nil
		case !errors.Is(dbErr, store.ErrNotFound):
			return nil, toAPIError(dbErr)
		}
	}
	return nil, s.workflowCallError(ctx, id, err)
}

// CloseBill closes an open bill before its period ends. It returns once the
// total is frozen, with the total being charged and the line items it covers.
// Charging happens asynchronously: poll GET /bills/:id for the outcome.
// Closing an already closing or closed bill returns it unchanged, so retries
// are safe.
//
//encore:api auth method=POST path=/bills/:id/close
func (s *Service) CloseBill(ctx context.Context, id uuid.UUID) (*BillResponse, error) {
	ctx, cancel := context.WithTimeout(ctx, workflowCallTimeout)
	defer cancel()

	// No UpdateID: the handler is idempotent, and Temporal would replay the
	// first close's (possibly outdated) result for a repeated ID.
	handle, err := s.temporal.UpdateWorkflow(ctx, client.UpdateWorkflowOptions{
		WorkflowID:   billworkflow.WorkflowID(id),
		UpdateName:   billworkflow.UpdateClose,
		WaitForStage: client.WorkflowUpdateStageCompleted,
	})
	if err == nil {
		var bill domain.Bill
		if err = handle.Get(ctx, &bill); err == nil {
			return newBillResponse(bill), nil
		}
	}

	// A completed workflow means the bill is already CLOSED (or never
	// existed, which GetBill reports as 404).
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		return s.GetBill(ctx, id)
	}
	return nil, s.workflowCallError(ctx, id, err)
}

// RetryCharge retries closing a bill whose charge failed.
//
//encore:api auth method=POST path=/bills/:id/retry-charge
func (s *Service) RetryCharge(ctx context.Context, id uuid.UUID) (*BillResponse, error) {
	bill, err := s.GetBill(ctx, id)
	if err != nil {
		return nil, err
	}
	if bill.Status != domain.StatusFailed {
		return nil, errs.B().Code(errs.Aborted).
			Msgf("only %s bills can be retried, bill is %s", domain.StatusFailed, bill.Status).Err()
	}

	err = s.temporal.SignalWorkflow(ctx, billworkflow.WorkflowID(id), "", billworkflow.SignalRetryCharge, nil)
	if err != nil {
		return nil, s.workflowCallError(ctx, id, err)
	}
	bill.Status = domain.StatusClosing
	return bill, nil
}

// GetBill returns a bill with its line items and, once closing, its total.
//
//encore:api auth method=GET path=/bills/:id
func (s *Service) GetBill(ctx context.Context, id uuid.UUID) (*BillResponse, error) {
	bill, err := s.getBill(ctx, id)
	if err != nil {
		return nil, err
	}
	return newBillResponse(bill), nil
}

// getBill reads a bill from the database, falling back to its workflow.
func (s *Service) getBill(ctx context.Context, id uuid.UUID) (domain.Bill, error) {
	bill, err := s.store.GetBill(ctx, id)
	if err == nil {
		return bill, nil
	}
	if !errors.Is(err, store.ErrNotFound) {
		return domain.Bill{}, toAPIError(err)
	}

	// Right after CreateBill the workflow may not have stored the bill yet;
	// ask the workflow directly before reporting 404.
	res, err := s.temporal.QueryWorkflow(ctx, billworkflow.WorkflowID(id), "", billworkflow.QueryGetBill)
	if err != nil {
		var notFound *serviceerror.NotFound
		if errors.As(err, &notFound) {
			return domain.Bill{}, errs.B().Code(errs.NotFound).Msg("bill not found").Err()
		}
		return domain.Bill{}, errs.WrapCode(err, errs.Unavailable, "query bill workflow")
	}
	if err := res.Get(&bill); err != nil {
		return domain.Bill{}, errs.WrapCode(err, errs.Internal, "decode bill")
	}
	return bill, nil
}

// ListBills lists bills, newest period first, optionally filtered by account
// and status.
//
//encore:api auth method=GET path=/bills
func (s *Service) ListBills(ctx context.Context, req *ListBillsRequest) (*ListBillsResponse, error) {
	bills, err := s.store.ListBills(ctx, store.ListBillsFilter{
		AccountID: req.AccountID,
		Status:    domain.Status(req.Status),
		Limit:     req.Limit,
	})
	if err != nil {
		return nil, toAPIError(err)
	}
	resp := &ListBillsResponse{Bills: make([]*BillResponse, 0, len(bills))}
	for _, b := range bills {
		resp.Bills = append(resp.Bills, newBillResponse(b))
	}
	return resp, nil
}

// ListCurrencies lists the currencies bills can be opened in.
//
//encore:api auth method=GET path=/currencies
func (s *Service) ListCurrencies(ctx context.Context) (*ListCurrenciesResponse, error) {
	currencies, err := s.store.ListCurrencies(ctx)
	if err != nil {
		return nil, toAPIError(err)
	}
	resp := &ListCurrenciesResponse{Currencies: make([]CurrencyResponse, 0, len(currencies))}
	for _, c := range currencies {
		resp.Currencies = append(resp.Currencies, CurrencyResponse{
			Code:       c.Code,
			Name:       c.Name,
			MinorUnits: c.MinorUnits,
		})
	}
	return resp, nil
}

// workflowCallError maps an error from signalling/updating a bill workflow.
// A workflow that doesn't exist or has completed is told apart using the DB:
// a stored bill means it is closed (409), no bill means 404.
func (s *Service) workflowCallError(ctx context.Context, id uuid.UUID, err error) error {
	var notFound *serviceerror.NotFound
	if errors.As(err, &notFound) {
		if _, dbErr := s.store.GetBill(ctx, id); dbErr == nil {
			return errs.B().Code(errs.Aborted).Msg("bill is closed").Err()
		}
		return errs.B().Code(errs.NotFound).Msg("bill not found").Err()
	}
	if errors.Is(err, context.DeadlineExceeded) {
		return errs.WrapCode(err, errs.Unavailable, "bill workflow did not respond in time; retry with the same Idempotency-Key")
	}
	return toAPIError(err)
}

// toAPIError maps domain, store and workflow errors to HTTP status codes:
// invalid input → 400, wrong state → 409, missing → 404, rest → 500.
func toAPIError(err error) error {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		switch appErr.Type() {
		case billworkflow.ErrTypeBillNotOpen:
			return errs.B().Code(errs.Aborted).Msg("bill is not open").Err()
		case billworkflow.ErrTypeCurrencyMismatch,
			billworkflow.ErrTypeInvalidAmount,
			billworkflow.ErrTypeInvalidBill,
			billworkflow.ErrTypeKeyReused:
			return errs.B().Code(errs.InvalidArgument).Msg(appErr.Message()).Err()
		case billworkflow.ErrTypeNegativeTotal:
			return errs.B().Code(errs.FailedPrecondition).Msg(appErr.Message()).Err()
		case billworkflow.ErrTypeNotFound:
			return errs.B().Code(errs.NotFound).Msg("bill not found").Err()
		}
	}

	switch {
	case errors.Is(err, domain.ErrInvalidPeriod),
		errors.Is(err, domain.ErrInvalidAmount),
		errors.Is(err, money.ErrUnsupportedCurrency),
		errors.Is(err, money.ErrCurrencyMismatch),
		// 422 would fit better, but errs has no such code; 400 is also
		// what Stripe returns for this.
		errors.Is(err, domain.ErrIdempotencyKeyReused):
		return errs.B().Code(errs.InvalidArgument).Msg(err.Error()).Err()
	case errors.Is(err, domain.ErrBillNotOpen), errors.Is(err, store.ErrBillNotOpen):
		return errs.B().Code(errs.Aborted).Msg("bill is not open").Err()
	case errors.Is(err, store.ErrNotFound):
		return errs.B().Code(errs.NotFound).Msg("bill not found").Err()
	}
	return errs.WrapCode(err, errs.Internal, "internal error")
}
