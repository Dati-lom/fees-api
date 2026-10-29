package domain

import (
	"errors"
	"fmt"
	"slices"
	"time"

	"encore.app/fees/money"
	"encore.dev/types/uuid"
)

type Status string

const (
	StatusOpen    Status = "OPEN"
	StatusClosing Status = "CLOSING"
	StatusClosed  Status = "CLOSED"
	StatusFailed  Status = "CHARGE_FAILED"
)

var transitions = map[Status][]Status{
	StatusOpen:    {StatusClosing},
	StatusClosing: {StatusClosed, StatusFailed},
	StatusFailed:  {StatusClosing}, //logic to retry and go to closing
}

var (
	ErrBillNotOpen       = errors.New("bill is not open")
	ErrInvalidTransition = errors.New("invalid status transition")
	ErrInvalidAmount     = errors.New("line item amount must be non-zero")
	ErrInvalidPeriod     = errors.New("period end must be after period start")
	// ErrIdempotencyKeyReused means a request reused an idempotency key with
	// a different body. Returning the original result would hide the
	// caller's bug, so it is rejected instead.
	ErrIdempotencyKeyReused = errors.New("idempotency key was already used with a different request")
)

func (s Status) CanTransitionTo(next Status) bool {
	return slices.Contains(transitions[s], next)
}

// AcceptsLineItems reports whether line items can still be added.
func (s Status) AcceptsLineItems() bool { return s == StatusOpen }

func (s Status) IsTerminal() bool { return s == StatusClosed }

// Bill is the domain entity. It has no JSON tags on purpose: the wire format
// lives in the API DTOs (fees/dto.go), so the domain can change independently.
type Bill struct {
	ID          uuid.UUID
	AccountID   string
	Currency    money.CurrencyCode
	Status      Status
	PeriodStart time.Time
	PeriodEnd   time.Time
	Total       *money.Money // nil until StartClosing (NULL in DB)
	CreatedAt   time.Time
	ClosedAt    *time.Time // nil until CLOSED
	LineItems   []LineItem
}

type LineItem struct {
	ID          uuid.UUID
	BillID      uuid.UUID
	Description string
	// Amount is positive for a fee and negative for a refund/credit.
	Amount money.Money
	// IdempotencyKey is supplied by the caller so retried requests don't double-charge.
	IdempotencyKey string
	CreatedAt      time.Time
}

// NewBillParams holds the inputs for NewBill. ID and Now are passed in rather
// than generated here so the caller controls them (Temporal workflows must use
// workflow.Now, not time.Now, to stay deterministic).
type NewBillParams struct {
	ID          uuid.UUID
	AccountID   string
	Currency    money.CurrencyCode
	PeriodStart time.Time
	PeriodEnd   time.Time
	Now         time.Time
}

// NewBill validates p and returns an OPEN bill with no line items.
//
// Whether p.Currency is supported is not checked here: that is a lookup in
// the currencies table, which the API does before starting the workflow.
// NewBill also runs inside the workflow, and workflow code must give the same
// result on every replay, so it can't depend on data that may change.
func NewBill(p NewBillParams) (Bill, error) {
	if !p.PeriodEnd.After(p.PeriodStart) {
		return Bill{}, ErrInvalidPeriod
	}
	return Bill{
		ID:          p.ID,
		AccountID:   p.AccountID,
		Currency:    p.Currency,
		Status:      StatusOpen,
		PeriodStart: p.PeriodStart,
		PeriodEnd:   p.PeriodEnd,
		CreatedAt:   p.Now,
		LineItems:   []LineItem{},
	}, nil
}

// Matches reports whether b is the bill p describes, i.e. whether p is a
// genuine retry of the request that created b. ID and Now are ignored: the ID
// is derived from the idempotency key and Now differs on every retry.
func (p NewBillParams) Matches(b Bill) bool {
	return p.AccountID == b.AccountID &&
		p.Currency == b.Currency &&
		p.PeriodStart.Equal(b.PeriodStart) &&
		p.PeriodEnd.Equal(b.PeriodEnd)
}

// ValidateLineItem checks that item can be added to b.
func (b *Bill) ValidateLineItem(item LineItem) error {
	if !b.Status.AcceptsLineItems() {
		return fmt.Errorf("%w: status is %s", ErrBillNotOpen, b.Status)
	}
	if item.Amount.Currency != b.Currency {
		return fmt.Errorf("%w: bill is %s, item is %s", money.ErrCurrencyMismatch, b.Currency, item.Amount.Currency)
	}
	if item.Amount.IsZero() {
		return ErrInvalidAmount
	}
	return nil
}

// AddLineItem validates and appends item to the bill.
func (b *Bill) AddLineItem(item LineItem) error {
	if err := b.ValidateLineItem(item); err != nil {
		return err
	}
	b.LineItems = append(b.LineItems, item)
	return nil
}

// ComputeTotal sums all line items in the bill's currency.
func (b *Bill) ComputeTotal() (money.Money, error) {
	total := money.Zero(b.Currency)
	for _, li := range b.LineItems {
		var err error
		if total, err = total.Add(li.Amount); err != nil {
			return money.Money{}, err
		}
	}
	return total, nil
}

// TransitionTo moves the bill to next, enforcing the state machine.
func (b *Bill) TransitionTo(next Status) error {
	if !b.Status.CanTransitionTo(next) {
		return fmt.Errorf("%w: %s -> %s", ErrInvalidTransition, b.Status, next)
	}
	b.Status = next
	return nil
}

// StartClosing stops accepting line items. Also used to retry from
// CHARGE_FAILED. Call FreezeTotal once the items accepted before this point
// have settled.
func (b *Bill) StartClosing() error {
	return b.TransitionTo(StatusClosing)
}

// FreezeTotal fixes the total that will be charged. A negative total means
// the account is owed a net credit. On a retry from CHARGE_FAILED items
// can't have changed, so the recomputed total is the same.
func (b *Bill) FreezeTotal() error {
	if b.Status != StatusClosing {
		return fmt.Errorf("%w: can only freeze the total while %s, bill is %s",
			ErrInvalidTransition, StatusClosing, b.Status)
	}
	total, err := b.ComputeTotal()
	if err != nil {
		return err
	}
	b.Total = &total
	return nil
}

// Close marks the frozen total as successfully charged.
func (b *Bill) Close(now time.Time) error {
	if err := b.TransitionTo(StatusClosed); err != nil {
		return err
	}
	b.ClosedAt = &now
	return nil
}

// MarkChargeFailed records that charging the frozen total failed.
func (b *Bill) MarkChargeFailed() error {
	return b.TransitionTo(StatusFailed)
}
