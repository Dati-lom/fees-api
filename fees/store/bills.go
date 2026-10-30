package store

import (
	"context"
	"errors"
	"fmt"
	"time"

	"encore.app/fees/domain"
	"encore.app/fees/money"
	"encore.dev/storage/sqldb"
	"encore.dev/types/uuid"
)

// InsertBill stores a new bill. Inserting the same bill ID again is a no-op,
// so a retried activity doesn't fail.
func (s *Store) InsertBill(ctx context.Context, b domain.Bill) error {
	return s.writeTx(ctx, func(tx *sqldb.Tx) error {
		_, err := tx.Exec(ctx, `
			INSERT INTO bills (id, account_id, currency, status, period_start, period_end, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (id) DO NOTHING
		`, b.ID, b.AccountID, b.Currency, b.Status, b.PeriodStart, b.PeriodEnd, b.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert bill: %w", err)
		}
		return nil
	})
}

// InsertLineItem adds an item to an OPEN bill. An item with the same
// (bill_id, idempotency_key) already stored is a no-op.
//
// The status check and the insert run in one serializable transaction. A
// concurrent StartClosing reads line_items and writes bills, while this reads
// bills and writes line_items; under weaker isolation both could commit
// (write skew) and the item would miss the frozen total. SERIALIZABLE makes
// Postgres abort one of them.
func (s *Store) InsertLineItem(ctx context.Context, item domain.LineItem) error {
	return s.writeTx(ctx, func(tx *sqldb.Tx) error {
		status, err := billStatus(ctx, tx, item.BillID)
		if err != nil {
			return err
		}
		if !status.AcceptsLineItems() {
			return fmt.Errorf("%w: status is %s", ErrBillNotOpen, status)
		}

		_, err = tx.Exec(ctx, `
			INSERT INTO line_items (id, bill_id, description, amount, currency, idempotency_key, created_at)
			VALUES ($1, $2, $3, $4, $5, $6, $7)
			ON CONFLICT (bill_id, idempotency_key) DO NOTHING
		`, item.ID, item.BillID, item.Description, item.Amount.Amount, item.Amount.Currency,
			item.IdempotencyKey, item.CreatedAt)
		if err != nil {
			return fmt.Errorf("insert line item: %w", err)
		}
		return nil
	})
}

// StartClosing moves a bill from OPEN (or CHARGE_FAILED, for a retry) to
// CLOSING and stores its frozen total.
//
// As a financial integrity check, total must equal the sum of the bill's line
// items in the database; otherwise ErrTotalMismatch is returned and nothing
// changes. Repeating the call once the bill is CLOSING with the same total is
// a no-op.
func (s *Store) StartClosing(ctx context.Context, billID uuid.UUID, total money.Money) error {
	return s.writeTx(ctx, func(tx *sqldb.Tx) error {
		var dbSum int64
		err := tx.QueryRow(ctx, `
			SELECT COALESCE(SUM(amount), 0) FROM line_items WHERE bill_id = $1
		`, billID).Scan(&dbSum)
		if err != nil {
			return fmt.Errorf("sum line items: %w", err)
		}
		if dbSum != total.Amount {
			return fmt.Errorf("%w: workflow total %d, database sum %d", ErrTotalMismatch, total.Amount, dbSum)
		}

		res, err := tx.Exec(ctx, `
			UPDATE bills SET status = $2, total_amount = $3
			WHERE id = $1 AND currency = $4 AND status IN ($5, $6)
		`, billID, domain.StatusClosing, total.Amount, total.Currency, domain.StatusOpen, domain.StatusFailed)
		if err != nil {
			return fmt.Errorf("start closing: %w", err)
		}
		if res.RowsAffected() == 1 {
			return nil
		}
		return requireStatus(ctx, tx, billID, domain.StatusClosing)
	})
}

// MarkClosed moves a bill from CLOSING to CLOSED. Repeating it is a no-op.
func (s *Store) MarkClosed(ctx context.Context, billID uuid.UUID, closedAt time.Time) error {
	return s.writeTx(ctx, func(tx *sqldb.Tx) error {
		res, err := tx.Exec(ctx, `
			UPDATE bills SET status = $2, closed_at = $3
			WHERE id = $1 AND status = $4
		`, billID, domain.StatusClosed, closedAt, domain.StatusClosing)
		if err != nil {
			return fmt.Errorf("mark closed: %w", err)
		}
		if res.RowsAffected() == 1 {
			return nil
		}
		return requireStatus(ctx, tx, billID, domain.StatusClosed)
	})
}

// MarkChargeFailed moves a bill from CLOSING to CHARGE_FAILED. Repeating it
// is a no-op.
func (s *Store) MarkChargeFailed(ctx context.Context, billID uuid.UUID) error {
	return s.writeTx(ctx, func(tx *sqldb.Tx) error {
		res, err := tx.Exec(ctx, `
			UPDATE bills SET status = $2
			WHERE id = $1 AND status = $3
		`, billID, domain.StatusFailed, domain.StatusClosing)
		if err != nil {
			return fmt.Errorf("mark charge failed: %w", err)
		}
		if res.RowsAffected() == 1 {
			return nil
		}
		return requireStatus(ctx, tx, billID, domain.StatusFailed)
	})
}

// GetBill returns a bill with all its line items, read from one snapshot.
func (s *Store) GetBill(ctx context.Context, billID uuid.UUID) (domain.Bill, error) {
	var b domain.Bill
	err := s.readTx(ctx, func(tx *sqldb.Tx) error {
		row := tx.QueryRow(ctx, `
			SELECT id, account_id, currency, status, period_start, period_end,
			       total_amount, created_at, closed_at
			FROM bills WHERE id = $1
		`, billID)
		var err error
		if b, err = scanBill(row); err != nil {
			return err
		}
		b.LineItems, err = lineItems(ctx, tx, billID)
		return err
	})
	return b, err
}

// GetLineItemByKey returns the item stored on billID under idempotency key
// key, or ErrNotFound.
func (s *Store) GetLineItemByKey(ctx context.Context, billID uuid.UUID, key string) (domain.LineItem, error) {
	var li domain.LineItem
	err := s.db.QueryRow(ctx, `
		SELECT id, bill_id, description, amount, currency, idempotency_key, created_at
		FROM line_items WHERE bill_id = $1 AND idempotency_key = $2
	`, billID, key).Scan(&li.ID, &li.BillID, &li.Description, &li.Amount.Amount, &li.Amount.Currency,
		&li.IdempotencyKey, &li.CreatedAt)
	if errors.Is(err, sqldb.ErrNoRows) {
		return domain.LineItem{}, ErrNotFound
	}
	if err != nil {
		return domain.LineItem{}, fmt.Errorf("get line item: %w", err)
	}
	return li, nil
}

// ListBillsFilter selects bills for ListBills. Zero values mean "any".
type ListBillsFilter struct {
	AccountID string
	Status    domain.Status
	Limit     int
}

const (
	defaultListLimit = 50
	maxListLimit     = 200
)

// ListBills returns bills newest period first, without their line items.
func (s *Store) ListBills(ctx context.Context, f ListBillsFilter) ([]domain.Bill, error) {
	limit := f.Limit
	if limit <= 0 {
		limit = defaultListLimit
	}
	limit = min(limit, maxListLimit)

	rows, err := s.db.Query(ctx, `
		SELECT id, account_id, currency, status, period_start, period_end,
		       total_amount, created_at, closed_at
		FROM bills
		WHERE ($1 = '' OR account_id = $1)
		  AND ($2 = '' OR status = $2)
		ORDER BY period_start DESC, id
		LIMIT $3
	`, f.AccountID, f.Status, limit)
	if err != nil {
		return nil, fmt.Errorf("list bills: %w", err)
	}
	defer rows.Close()

	bills := []domain.Bill{}
	for rows.Next() {
		b, err := scanBill(rows)
		if err != nil {
			return nil, err
		}
		bills = append(bills, b)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list bills: %w", err)
	}
	return bills, nil
}

type scanner interface {
	Scan(dest ...any) error
}

func scanBill(row scanner) (domain.Bill, error) {
	var (
		b     domain.Bill
		total *int64
	)
	err := row.Scan(&b.ID, &b.AccountID, &b.Currency, &b.Status, &b.PeriodStart, &b.PeriodEnd,
		&total, &b.CreatedAt, &b.ClosedAt)
	if errors.Is(err, sqldb.ErrNoRows) {
		return domain.Bill{}, ErrNotFound
	}
	if err != nil {
		return domain.Bill{}, fmt.Errorf("scan bill: %w", err)
	}
	if total != nil {
		b.Total = &money.Money{Amount: *total, Currency: b.Currency}
	}
	return b, nil
}

func lineItems(ctx context.Context, tx *sqldb.Tx, billID uuid.UUID) ([]domain.LineItem, error) {
	rows, err := tx.Query(ctx, `
		SELECT id, bill_id, description, amount, currency, idempotency_key, created_at
		FROM line_items WHERE bill_id = $1
		ORDER BY created_at, id
	`, billID)
	if err != nil {
		return nil, fmt.Errorf("list line items: %w", err)
	}
	defer rows.Close()

	var items []domain.LineItem
	for rows.Next() {
		var li domain.LineItem
		err := rows.Scan(&li.ID, &li.BillID, &li.Description, &li.Amount.Amount, &li.Amount.Currency,
			&li.IdempotencyKey, &li.CreatedAt)
		if err != nil {
			return nil, fmt.Errorf("scan line item: %w", err)
		}
		items = append(items, li)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("list line items: %w", err)
	}
	return items, nil
}

func billStatus(ctx context.Context, tx *sqldb.Tx, billID uuid.UUID) (domain.Status, error) {
	var status domain.Status
	err := tx.QueryRow(ctx, `SELECT status FROM bills WHERE id = $1`, billID).Scan(&status)
	if errors.Is(err, sqldb.ErrNoRows) {
		return "", ErrNotFound
	}
	if err != nil {
		return "", fmt.Errorf("get bill status: %w", err)
	}
	return status, nil
}

// requireStatus is used when a conditional UPDATE matched no rows: it returns
// nil if the bill is already in want (the write was a retry) and an error
// otherwise.
func requireStatus(ctx context.Context, tx *sqldb.Tx, billID uuid.UUID, want domain.Status) error {
	status, err := billStatus(ctx, tx, billID)
	if err != nil {
		return err
	}
	if status != want {
		return fmt.Errorf("%w: bill is %s, expected %s", ErrInvalidTransition, status, want)
	}
	return nil
}
