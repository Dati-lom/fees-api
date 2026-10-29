package store

// These tests run against a real Postgres (run them with `encore test`). They
// check what Temporal relies on when it retries an activity: every write can
// run more than once with the same result, and a write that overlaps another
// one for the same bill can't leave the bill inconsistent.

import (
	"context"
	"errors"
	"sync"
	"testing"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.dev/et"
	"encore.dev/types/uuid"
	"github.com/stretchr/testify/require"
)

var periodStart = time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)

func newStore(t *testing.T) *Store {
	t.Helper()
	db, err := et.NewTestDatabase(context.Background(), "fees")
	require.NoError(t, err)
	return New(db)
}

func newID(t *testing.T) uuid.UUID {
	t.Helper()
	id, err := uuid.NewV4()
	require.NoError(t, err)
	return id
}

func gel(amount int64) money.Money { return money.Money{Amount: amount, Currency: "GEL"} }

// insertBill stores a new OPEN GEL bill.
func insertBill(t *testing.T, s *Store) domain.Bill {
	t.Helper()
	b, err := domain.NewBill(domain.NewBillParams{
		ID:          newID(t),
		AccountID:   "acc-1",
		Currency:    "GEL",
		PeriodStart: periodStart,
		PeriodEnd:   periodStart.AddDate(0, 1, 0),
		Now:         periodStart,
	})
	require.NoError(t, err)
	require.NoError(t, s.InsertBill(context.Background(), b))
	return b
}

func lineItem(t *testing.T, billID uuid.UUID, key string, amount int64) domain.LineItem {
	t.Helper()
	return domain.LineItem{
		ID:             newID(t),
		BillID:         billID,
		Description:    key,
		Amount:         gel(amount),
		IdempotencyKey: key,
		CreatedAt:      periodStart,
	}
}

func TestInsertBillRetryIsNoop(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)

	require.NoError(t, s.InsertBill(ctx, b))

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusOpen, got.Status)
}

func TestInsertLineItemRetryIsNoop(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	item := lineItem(t, b.ID, "fee-1", 500)

	require.NoError(t, s.InsertLineItem(ctx, item))
	require.NoError(t, s.InsertLineItem(ctx, item), "activity retry")
	// Same idempotency key under a new row ID is still the same item.
	again := item
	again.ID = newID(t)
	require.NoError(t, s.InsertLineItem(ctx, again))

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Len(t, got.LineItems, 1)
	require.Equal(t, item.ID, got.LineItems[0].ID)
}

func TestInsertLineItemRejectedOnceClosing(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	require.NoError(t, s.StartClosing(ctx, b.ID, gel(0)))

	err := s.InsertLineItem(ctx, lineItem(t, b.ID, "late", 500))
	require.ErrorIs(t, err, ErrBillNotOpen)
}

func TestInsertLineItemUnknownBill(t *testing.T) {
	s := newStore(t)
	err := s.InsertLineItem(context.Background(), lineItem(t, newID(t), "fee", 500))
	require.ErrorIs(t, err, ErrNotFound)
}

func TestStartClosingRetryIsNoop(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	require.NoError(t, s.InsertLineItem(ctx, lineItem(t, b.ID, "fee", 500)))

	require.NoError(t, s.StartClosing(ctx, b.ID, gel(500)))
	require.NoError(t, s.StartClosing(ctx, b.ID, gel(500)), "activity retry")

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusClosing, got.Status)
	require.Equal(t, gel(500), *got.Total)
}

func TestStartClosingRejectsTotalMismatch(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	require.NoError(t, s.InsertLineItem(ctx, lineItem(t, b.ID, "fee", 500)))

	err := s.StartClosing(ctx, b.ID, gel(400))
	require.ErrorIs(t, err, ErrTotalMismatch)

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusOpen, got.Status, "nothing changes on a mismatch")
	require.Nil(t, got.Total)
}

func TestMarkClosedRetryIsNoop(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	require.NoError(t, s.StartClosing(ctx, b.ID, gel(0)))
	closedAt := periodStart.AddDate(0, 1, 0)

	require.NoError(t, s.MarkClosed(ctx, b.ID, closedAt))
	require.NoError(t, s.MarkClosed(ctx, b.ID, closedAt), "activity retry")

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusClosed, got.Status)
	require.True(t, closedAt.Equal(*got.ClosedAt))
}

func TestMarkClosedRequiresClosing(t *testing.T) {
	s := newStore(t)
	b := insertBill(t, s)

	err := s.MarkClosed(context.Background(), b.ID, periodStart)
	require.ErrorIs(t, err, ErrInvalidTransition, "an OPEN bill can't jump to CLOSED")
}

// A failed charge followed by a retry walks CLOSING → CHARGE_FAILED →
// CLOSING → CLOSED, and every step can be repeated.
func TestChargeFailedThenRetryCloses(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)
	b := insertBill(t, s)
	require.NoError(t, s.InsertLineItem(ctx, lineItem(t, b.ID, "fee", 500)))
	require.NoError(t, s.StartClosing(ctx, b.ID, gel(500)))

	require.NoError(t, s.MarkChargeFailed(ctx, b.ID))
	require.NoError(t, s.MarkChargeFailed(ctx, b.ID), "activity retry")

	// Items can't be added while CHARGE_FAILED either.
	require.ErrorIs(t, s.InsertLineItem(ctx, lineItem(t, b.ID, "late", 100)), ErrBillNotOpen)

	require.NoError(t, s.StartClosing(ctx, b.ID, gel(500)))
	require.NoError(t, s.MarkClosed(ctx, b.ID, periodStart))

	got, err := s.GetBill(ctx, b.ID)
	require.NoError(t, err)
	require.Equal(t, domain.StatusClosed, got.Status)
	require.Equal(t, gel(500), *got.Total)
}

// TestConcurrentInsertAndCloseNeverLosesItem races a line item insert against
// closing the bill, many times. Exactly one of them may win:
//   - the close wins: the insert fails with ErrBillNotOpen;
//   - the insert wins: the close sees a sum that no longer matches its total
//     and fails with ErrTotalMismatch.
//
// Both succeeding would be write skew: an item stored on a bill whose total
// was frozen without it. SERIALIZABLE is what rules that out.
func TestConcurrentInsertAndCloseNeverLosesItem(t *testing.T) {
	ctx := context.Background()
	s := newStore(t)

	const rounds = 50
	var insertWon, closeWon int
	for range rounds {
		b := insertBill(t, s)
		require.NoError(t, s.InsertLineItem(ctx, lineItem(t, b.ID, "fee", 500)))

		var insertErr, closeErr error
		var wg sync.WaitGroup
		start := make(chan struct{})
		wg.Add(2)
		go func() {
			defer wg.Done()
			<-start
			insertErr = s.InsertLineItem(ctx, lineItem(t, b.ID, "racer", 300))
		}()
		go func() {
			defer wg.Done()
			<-start
			closeErr = s.StartClosing(ctx, b.ID, gel(500))
		}()
		close(start)
		wg.Wait()

		switch {
		case insertErr == nil && closeErr == nil:
			t.Fatalf("both the insert and the close committed (write skew) on bill %s", b.ID)
		case errors.Is(insertErr, ErrBillNotOpen) && closeErr == nil:
			closeWon++
		case insertErr == nil && errors.Is(closeErr, ErrTotalMismatch):
			insertWon++
		default:
			t.Fatalf("unexpected outcome: insert=%v close=%v", insertErr, closeErr)
		}

		// Whatever happened, a frozen total always equals the stored items.
		got, err := s.GetBill(ctx, b.ID)
		require.NoError(t, err)
		if got.Total != nil {
			var sum int64
			for _, li := range got.LineItems {
				sum += li.Amount.Amount
			}
			require.Equal(t, sum, got.Total.Amount)
		}
	}
	t.Logf("insert won %d, close won %d of %d rounds", insertWon, closeWon, rounds)
}
