package workflow

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.app/fees/payment"
	"encore.dev/types/uuid"
	"github.com/stretchr/testify/mock"
	"github.com/stretchr/testify/require"
	"go.temporal.io/sdk/temporal"
	"go.temporal.io/sdk/testsuite"
)

var (
	periodStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	periodEnd   = periodStart.AddDate(0, 1, 0)
)

// envOption customises newEnv. Options run before the default mocks are
// registered; testify uses the first matching expectation, so mocks set up
// in an option take precedence over the defaults.
type envOption func(env *testsuite.TestWorkflowEnvironment, a *Activities)

// withCharger runs the real ChargeBill activity against c.
func withCharger(c payment.Charger) envOption {
	return func(_ *testsuite.TestWorkflowEnvironment, a *Activities) { a.Charger = c }
}

// newEnv returns a test environment with the DB activities mocked. ChargeBill
// must be mocked by the test, unless withCharger is given.
func newEnv(t *testing.T, opts ...envOption) *testsuite.TestWorkflowEnvironment {
	t.Helper()
	var s testsuite.WorkflowTestSuite
	env := s.NewTestWorkflowEnvironment()
	env.SetStartTime(periodStart)
	a := &Activities{}
	env.RegisterActivity(a)
	for _, opt := range opts {
		opt(env, a)
	}
	env.OnActivity(acts.InsertBill, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(acts.InsertLineItem, mock.Anything, mock.Anything).Return(nil).Maybe()
	env.OnActivity(acts.StartClosing, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(acts.MarkClosed, mock.Anything, mock.Anything).Return(nil)
	env.OnActivity(acts.MarkChargeFailed, mock.Anything, mock.Anything).Return(nil).Maybe()
	return env
}

func params(t *testing.T, accountID string) domain.NewBillParams {
	return domain.NewBillParams{
		ID:          newID(t),
		AccountID:   accountID,
		Currency:    "GEL",
		PeriodStart: periodStart,
		PeriodEnd:   periodEnd,
		Now:         periodStart,
	}
}

func newID(t *testing.T) uuid.UUID {
	id, err := uuid.NewV4()
	require.NoError(t, err)
	return id
}

func gel(amount int64) money.Money { return money.Money{Amount: amount, Currency: "GEL"} }

// addItem sends UpdateAddLineItem after delay and records the outcome in *err.
func addItem(t *testing.T, env *testsuite.TestWorkflowEnvironment, delay time.Duration, key string, amount money.Money, outErr *error) {
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateAddLineItem, key, &testsuite.TestUpdateCallback{
			OnAccept:   func() {},
			OnReject:   func(err error) { *outErr = err },
			OnComplete: func(_ any, err error) { *outErr = err },
		}, AddLineItemInput{ID: newID(t), Description: key, Amount: amount, IdempotencyKey: key})
	}, delay)
}

// closeBill sends UpdateClose after delay and records the reply.
func closeBill(env *testsuite.TestWorkflowEnvironment, delay time.Duration, out *domain.Bill, outErr *error) {
	env.RegisterDelayedCallback(func() {
		env.UpdateWorkflow(UpdateClose, "", &testsuite.TestUpdateCallback{
			OnAccept: func() {},
			OnReject: func(err error) { *outErr = err },
			OnComplete: func(res any, err error) {
				*outErr = err
				if b, ok := res.(domain.Bill); ok {
					*out = b
				}
			},
		})
	}, delay)
}

func finalBill(t *testing.T, env *testsuite.TestWorkflowEnvironment) domain.Bill {
	t.Helper()
	res, err := env.QueryWorkflow(QueryGetBill)
	require.NoError(t, err)
	var b domain.Bill
	require.NoError(t, res.Get(&b))
	return b
}

func errType(err error) string {
	var appErr *temporal.ApplicationError
	if errors.As(err, &appErr) {
		return appErr.Type()
	}
	return ""
}

func TestBillClosesAtPeriodEndAndChargesNetTotal(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.MatchedBy(func(in ChargeBillInput) bool {
		return in.Amount == gel(1000)
	})).Return(ChargeBillResult{ChargeID: "ch_1"}, nil).Once()

	var err1, err2 error
	addItem(t, env, time.Hour, "fee", gel(1250), &err1)
	addItem(t, env, 2*time.Hour, "refund", gel(-250), &err2)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.True(t, env.IsWorkflowCompleted())
	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, err1)
	require.NoError(t, err2)
	b := finalBill(t, env)
	require.Equal(t, domain.StatusClosed, b.Status)
	require.Equal(t, gel(1000), *b.Total)
	require.Len(t, b.LineItems, 2)
	env.AssertExpectations(t)
}

func TestLineItemRejectedAfterClose(t *testing.T) {
	env := newEnv(t)
	// Charge takes a while, so the item below arrives while CLOSING.
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).
		After(time.Hour).Return(ChargeBillResult{ChargeID: "ch_1"}, nil)

	var feeErr, closeErr, lateErr error
	var closed domain.Bill
	addItem(t, env, time.Hour, "fee", gel(500), &feeErr)
	closeBill(env, 2*time.Hour, &closed, &closeErr)
	addItem(t, env, 2*time.Hour+time.Minute, "late", gel(100), &lateErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, feeErr)
	require.NoError(t, closeErr)
	require.Equal(t, ErrTypeBillNotOpen, errType(lateErr))
	b := finalBill(t, env)
	require.Equal(t, domain.StatusClosed, b.Status)
	require.Equal(t, gel(500), *b.Total)
	require.Len(t, b.LineItems, 1)
}

func TestDuplicateIdempotencyKeyIsAddedOnce(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(ChargeBillResult{}, nil)

	var err1, err2 error
	addItem(t, env, time.Hour, "same-key", gel(500), &err1)
	addItem(t, env, 2*time.Hour, "same-key", gel(500), &err2)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, err1)
	require.NoError(t, err2)
	b := finalBill(t, env)
	require.Len(t, b.LineItems, 1)
	require.Equal(t, gel(500), *b.Total)
	env.AssertNumberOfCalls(t, "InsertLineItem", 1)
}

func TestCurrencyMismatchRejected(t *testing.T) {
	env := newEnv(t)
	var err, closeErr error
	var closed domain.Bill
	addItem(t, env, time.Hour, "usd", money.Money{Amount: 100, Currency: "USD"}, &err)
	closeBill(env, 2*time.Hour, &closed, &closeErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.Equal(t, ErrTypeCurrencyMismatch, errType(err))
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
}

func TestDeclinedChargeThenRetryUsesNewIdempotencyKey(t *testing.T) {
	env := newEnv(t)
	declined := temporal.NewNonRetryableApplicationError("declined", ErrTypeChargeDeclined, nil)
	var keys []string
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ChargeBillInput) (ChargeBillResult, error) {
			keys = append(keys, in.IdempotencyKey)
			if len(keys) == 1 {
				return ChargeBillResult{}, declined
			}
			return ChargeBillResult{ChargeID: "ch_2"}, nil
		})

	var err error
	addItem(t, env, time.Hour, "fee", gel(500), &err)
	env.RegisterDelayedCallback(func() {
		require.Equal(t, domain.StatusFailed, finalBill(t, env).Status)
		env.SignalWorkflow(SignalRetryCharge, nil)
	}, 40*24*time.Hour) // after period end

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
	env.AssertNumberOfCalls(t, "MarkChargeFailed", 1)
	// A decline is final, so the retry is a new charge with a new key.
	require.Len(t, keys, 2)
	require.NotEqual(t, keys[0], keys[1])
}

func TestRefundExceedingTotalRejectedAndZeroTotalNotCharged(t *testing.T) {
	env := newEnv(t)
	var feeErr, tooBigErr, refundErr error
	addItem(t, env, time.Hour, "fee", gel(300), &feeErr)
	addItem(t, env, 2*time.Hour, "too-big", gel(-301), &tooBigErr)
	addItem(t, env, 3*time.Hour, "refund", gel(-300), &refundErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, feeErr)
	require.Equal(t, ErrTypeNegativeTotal, errType(tooBigErr))
	require.NoError(t, refundErr)
	b := finalBill(t, env)
	require.Equal(t, domain.StatusClosed, b.Status)
	require.Equal(t, gel(0), *b.Total)
	require.Len(t, b.LineItems, 2)
	env.AssertNotCalled(t, "ChargeBill", mock.Anything, mock.Anything)
}

func TestCloseReturnsFrozenTotalAndLineItems(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).
		After(time.Hour).Return(ChargeBillResult{ChargeID: "ch_1"}, nil)

	var err1, err2, closeErr, againErr error
	var closed, again domain.Bill
	addItem(t, env, time.Hour, "fee", gel(1250), &err1)
	addItem(t, env, 2*time.Hour, "refund", gel(-250), &err2)
	closeBill(env, 3*time.Hour, &closed, &closeErr)
	// Closing again while the charge is in progress returns the same bill.
	closeBill(env, 3*time.Hour+time.Minute, &again, &againErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, closeErr)
	require.Equal(t, domain.StatusClosing, closed.Status)
	require.NotNil(t, closed.Total)
	require.Equal(t, gel(1000), *closed.Total)
	require.Len(t, closed.LineItems, 2)

	require.NoError(t, againErr)
	require.Equal(t, gel(1000), *again.Total)
	require.Len(t, again.LineItems, 2)

	b := finalBill(t, env)
	require.Equal(t, domain.StatusClosed, b.Status)
	require.Equal(t, gel(1000), *b.Total)
	env.AssertNumberOfCalls(t, "ChargeBill", 1)
	env.AssertNumberOfCalls(t, "StartClosing", 1)
}

func TestCloseBeforeBillIsStoredWaitsForIt(t *testing.T) {
	env := newEnv(t, func(env *testsuite.TestWorkflowEnvironment, _ *Activities) {
		// InsertBill is slow, so the close arrives before the bill is stored.
		env.OnActivity(acts.InsertBill, mock.Anything, mock.Anything).After(time.Minute).Return(nil)
	})
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(ChargeBillResult{}, nil).Maybe()

	var closeErr error
	var closed domain.Bill
	closeBill(env, time.Second, &closed, &closeErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, closeErr)
	require.Equal(t, gel(0), *closed.Total)
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
	// The close waited for the slow insert, and still closed long before
	// the period end.
	require.GreaterOrEqual(t, env.Now().Sub(periodStart), time.Minute)
	require.Less(t, env.Now().Sub(periodStart), time.Hour)
}

func TestUnknownChargeOutcomeRetryReusesIdempotencyKey(t *testing.T) {
	env := newEnv(t)
	var keys []string
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(
		func(_ context.Context, in ChargeBillInput) (ChargeBillResult, error) {
			keys = append(keys, in.IdempotencyKey)
			if len(keys) <= int(chargeActivityOptions.RetryPolicy.MaximumAttempts) {
				return ChargeBillResult{}, payment.ErrUnavailable // retryable
			}
			return ChargeBillResult{ChargeID: "ch_1"}, nil
		})

	var err error
	addItem(t, env, time.Hour, "fee", gel(500), &err)
	env.RegisterDelayedCallback(func() {
		require.Equal(t, domain.StatusFailed, finalBill(t, env).Status)
		env.SignalWorkflow(SignalRetryCharge, nil)
	}, 40*24*time.Hour)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
	// 5 attempts of the first charge + 1 after the retry, all one charge.
	require.Len(t, keys, 6)
	for _, k := range keys {
		require.Equal(t, keys[0], k)
	}
}

// lostResponseProvider simulates a payment provider that processes a charge
// but whose responses don't reach us for the first `lost` calls. Like a real
// provider it deduplicates by idempotency key.
type lostResponseProvider struct {
	mu      sync.Mutex
	lost    int
	calls   int
	charges map[string]money.Money // by idempotency key
}

func (p *lostResponseProvider) Charge(_ context.Context, req payment.ChargeRequest) (payment.ChargeResult, error) {
	p.mu.Lock()
	defer p.mu.Unlock()
	p.calls++
	if _, ok := p.charges[req.IdempotencyKey]; !ok {
		p.charges[req.IdempotencyKey] = req.Amount // money moves here
	}
	if p.calls <= p.lost {
		return payment.ChargeResult{}, payment.ErrUnavailable
	}
	return payment.ChargeResult{ChargeID: "ch_" + req.IdempotencyKey}, nil
}

// TestChargeWithLostResponsesIsNotDuplicated runs the real ChargeBill
// activity. The provider charges on the first call, but every response of the
// first attempt is lost, so the bill ends CHARGE_FAILED. Retrying must not
// charge the customer a second time.
func TestChargeWithLostResponsesIsNotDuplicated(t *testing.T) {
	provider := &lostResponseProvider{
		lost:    int(chargeActivityOptions.RetryPolicy.MaximumAttempts),
		charges: map[string]money.Money{},
	}
	env := newEnv(t, withCharger(provider))

	var err error
	addItem(t, env, time.Hour, "fee", gel(500), &err)
	env.RegisterDelayedCallback(func() {
		require.Equal(t, domain.StatusFailed, finalBill(t, env).Status)
		env.SignalWorkflow(SignalRetryCharge, nil)
	}, 40*24*time.Hour)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
	require.Len(t, provider.charges, 1, "customer was charged more than once")
}

func TestReusedIdempotencyKeyWithDifferentBodyRejected(t *testing.T) {
	env := newEnv(t)
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(ChargeBillResult{}, nil)

	// Different update IDs, so Temporal's own dedup doesn't apply and the
	// workflow's validator sees both requests.
	send := func(delay time.Duration, updateID string, amount money.Money, outErr *error) {
		env.RegisterDelayedCallback(func() {
			env.UpdateWorkflow(UpdateAddLineItem, updateID, &testsuite.TestUpdateCallback{
				OnAccept:   func() {},
				OnReject:   func(err error) { *outErr = err },
				OnComplete: func(_ any, err error) { *outErr = err },
			}, AddLineItemInput{ID: newID(t), Description: "fee", Amount: amount, IdempotencyKey: "key-1"})
		}, delay)
	}
	var firstErr, sameErr, differentErr error
	send(time.Hour, "u1", gel(500), &firstErr)
	send(2*time.Hour, "u2", gel(500), &sameErr)
	send(3*time.Hour, "u3", gel(9900), &differentErr)

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, firstErr)
	require.NoError(t, sameErr)
	require.Equal(t, ErrTypeKeyReused, errType(differentErr))
	b := finalBill(t, env)
	require.Len(t, b.LineItems, 1)
	require.Equal(t, gel(500), *b.Total)
}

func TestAddLineItemInputMatches(t *testing.T) {
	item := domain.LineItem{ID: newID(t), Description: "fee", Amount: gel(500)}
	in := AddLineItemInput{ID: newID(t), Description: "fee", Amount: gel(500)}

	require.True(t, in.Matches(item), "a new ID on retry must not matter")
	in.Amount = gel(501)
	require.False(t, in.Matches(item))
	in.Amount = money.Money{Amount: 500, Currency: "USD"}
	require.False(t, in.Matches(item))
	in.Amount, in.Description = gel(500), "other"
	require.False(t, in.Matches(item))
}

// An item whose insert is still running when the bill closes, and then fails
// permanently, is dropped. The frozen total must not include it, or it won't
// match the line items in the database.
func TestItemFailingDuringCloseIsLeftOutOfTotal(t *testing.T) {
	isBad := mock.MatchedBy(func(li domain.LineItem) bool { return li.IdempotencyKey == "bad" })
	env := newEnv(t, func(env *testsuite.TestWorkflowEnvironment, _ *Activities) {
		env.OnActivity(acts.InsertLineItem, mock.Anything, isBad).After(time.Minute).
			Return(temporal.NewNonRetryableApplicationError("constraint violated", "Permanent", nil))
	})
	env.OnActivity(acts.ChargeBill, mock.Anything, mock.Anything).Return(ChargeBillResult{}, nil)

	var goodErr, badErr, closeErr error
	var closed domain.Bill
	addItem(t, env, time.Hour, "good", gel(500), &goodErr)
	addItem(t, env, 2*time.Hour, "bad", gel(9900), &badErr)
	closeBill(env, 2*time.Hour+time.Second, &closed, &closeErr) // bad insert in flight

	env.ExecuteWorkflow(BillWorkflow, params(t, "acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, goodErr)
	require.Error(t, badErr)
	require.NoError(t, closeErr)
	require.Equal(t, gel(500), *closed.Total)
	require.Len(t, closed.LineItems, 1)
	env.AssertCalled(t, "StartClosing", mock.Anything, StartClosingInput{BillID: closed.ID, Total: gel(500)})
	require.Equal(t, domain.StatusClosed, finalBill(t, env).Status)
}

// failFirst returns an activity mock that fails with a transient (retryable)
// error n times, then succeeds, counting calls in *calls.
func failFirst[In any](n int, calls *int) func(context.Context, In) error {
	return func(_ context.Context, _ In) error {
		*calls++
		if *calls <= n {
			return errors.New("connection reset by peer")
		}
		return nil
	}
}

// TestTransientFailuresAreRetried makes every DB write and the charge fail a
// couple of times before succeeding, as during a database or provider
// outage. Temporal retries each activity; the bill must still end CLOSED
// with the right total, every caller must get a success, and the customer
// must be charged exactly once.
func TestTransientFailuresAreRetried(t *testing.T) {
	var insertBill, insertItem, startClosing, markClosed int
	charger := payment.NewMockCharger()
	env := newEnv(t, withCharger(charger), func(env *testsuite.TestWorkflowEnvironment, _ *Activities) {
		env.OnActivity(acts.InsertBill, mock.Anything, mock.Anything).
			Return(failFirst[domain.Bill](2, &insertBill))
		env.OnActivity(acts.InsertLineItem, mock.Anything, mock.Anything).
			Return(failFirst[domain.LineItem](2, &insertItem))
		env.OnActivity(acts.StartClosing, mock.Anything, mock.Anything).
			Return(failFirst[StartClosingInput](2, &startClosing))
		env.OnActivity(acts.MarkClosed, mock.Anything, mock.Anything).
			Return(failFirst[MarkClosedInput](2, &markClosed))
	})

	var itemErr, closeErr error
	var closed domain.Bill
	addItem(t, env, time.Hour, "fee", gel(500), &itemErr)
	closeBill(env, 2*time.Hour, &closed, &closeErr)

	// "flaky-" makes the mock charger fail its first 2 attempts too.
	env.ExecuteWorkflow(BillWorkflow, params(t, payment.FlakyPrefix+"acc-1"))

	require.NoError(t, env.GetWorkflowError())
	require.NoError(t, itemErr)
	require.NoError(t, closeErr)
	require.Equal(t, gel(500), *closed.Total)

	b := finalBill(t, env)
	require.Equal(t, domain.StatusClosed, b.Status)
	require.Len(t, b.LineItems, 1, "a retried insert must not duplicate the item")
	require.Equal(t, gel(500), *b.Total)

	// Each write was attempted 3 times: 2 failures, then success.
	for name, n := range map[string]int{
		"InsertBill": insertBill, "InsertLineItem": insertItem,
		"StartClosing": startClosing, "MarkClosed": markClosed,
	} {
		require.Equal(t, 3, n, name)
	}
	env.AssertNotCalled(t, "MarkChargeFailed", mock.Anything, mock.Anything)
	require.Equal(t, 1, charger.Charges(), "customer must be charged exactly once")
}
