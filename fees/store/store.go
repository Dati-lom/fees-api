// Package store persists bills, line items and currencies in Postgres.
//
// Writes run in SERIALIZABLE transactions. Postgres may abort a serializable
// transaction with a serialization failure (40001) when it conflicts with a
// concurrent one; those are retried here a few times, and any error that is
// still returned is retried again by Temporal at the activity level.
//
// Every write is idempotent (safe to repeat), because Temporal activities are
// retried and can run more than once.
package store

import (
	"context"
	"errors"
	"fmt"

	"encore.dev/storage/sqldb"
)

var (
	ErrNotFound          = errors.New("store: not found")
	ErrBillNotOpen       = errors.New("store: bill is not open")
	ErrInvalidTransition = errors.New("store: invalid status transition")
	// ErrTotalMismatch means the total computed by the workflow differs from
	// the sum of line items in the database. It should never happen; if it
	// does, closing is refused rather than charging a wrong amount.
	ErrTotalMismatch = errors.New("store: total does not match line items")
)

type Store struct {
	db *sqldb.Database
}

func New(db *sqldb.Database) *Store {
	return &Store{db: db}
}

const maxTxAttempts = 3

// writeTx runs fn in a SERIALIZABLE transaction, retrying on serialization
// failures and deadlocks.
func (s *Store) writeTx(ctx context.Context, fn func(tx *sqldb.Tx) error) error {
	return s.runTx(ctx, "SET TRANSACTION ISOLATION LEVEL SERIALIZABLE", fn)
}

// readTx runs fn in a REPEATABLE READ, READ ONLY transaction: every query sees
// the same snapshot, so a bill and its line items are always consistent.
// Serializable isn't needed for a read-only snapshot and would add aborts.
func (s *Store) readTx(ctx context.Context, fn func(tx *sqldb.Tx) error) error {
	return s.runTx(ctx, "SET TRANSACTION ISOLATION LEVEL REPEATABLE READ, READ ONLY", fn)
}

func (s *Store) runTx(ctx context.Context, setTx string, fn func(tx *sqldb.Tx) error) error {
	var err error
	for range maxTxAttempts {
		if err = s.tryTx(ctx, setTx, fn); err == nil || !isRetryable(err) {
			return err
		}
	}
	return fmt.Errorf("transaction failed after %d attempts: %w", maxTxAttempts, err)
}

func (s *Store) tryTx(ctx context.Context, setTx string, fn func(tx *sqldb.Tx) error) error {
	tx, err := s.db.Begin(ctx)
	if err != nil {
		return err
	}
	// No-op after a successful Commit.
	defer func() { _ = tx.Rollback() }()

	if _, err := tx.Exec(ctx, setTx); err != nil {
		return err
	}
	if err := fn(tx); err != nil {
		return err
	}
	return tx.Commit()
}

// isRetryable reports whether err is a Postgres serialization failure or
// deadlock, both of which succeed if the transaction is simply run again.
func isRetryable(err error) bool {
	var dbErr *sqldb.Error
	if !errors.As(err, &dbErr) {
		return false
	}
	switch dbErr.DatabaseCode {
	case "40001", // serialization_failure
		"40P01": // deadlock_detected
		return true
	}
	return false
}

// IsPermanent reports whether err is a Postgres error that fails the same way
// however often it is retried: bad data, a violated constraint or a broken
// query. Connection, resource and transaction-rollback errors are not
// permanent; they go away once the database recovers.
func IsPermanent(err error) bool {
	var dbErr *sqldb.Error
	if !errors.As(err, &dbErr) || len(dbErr.DatabaseCode) < 2 {
		return false
	}
	// The first two characters of a SQLSTATE are its class.
	switch dbErr.DatabaseCode[:2] {
	case "22", // data_exception
		"23", // integrity_constraint_violation
		"42": // syntax_error_or_access_rule_violation
		return true
	}
	return false
}
