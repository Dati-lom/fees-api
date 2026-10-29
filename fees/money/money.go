package money

import (
	"errors"
	"fmt"
	"math"
)

// Money is an amount in minor units (1250 GEL = 12.50 GEL).
type Money struct {
	Amount   int64        `json:"amount"`
	Currency CurrencyCode `json:"currency"`
}

var (
	ErrCurrencyMismatch = errors.New("money: currency mismatch")
	ErrOverflow         = errors.New("money: overflow")
)

// Zero returns a zero amount in currency c.
func Zero(c CurrencyCode) Money { return Money{Currency: c} }

func (m Money) Add(o Money) (Money, error) {
	if m.Currency != o.Currency {
		return Money{}, fmt.Errorf("%w: %s vs %s", ErrCurrencyMismatch, m.Currency, o.Currency)
	}
	if (o.Amount > 0 && m.Amount > math.MaxInt64-o.Amount) ||
		(o.Amount < 0 && m.Amount < math.MinInt64-o.Amount) {
		return Money{}, ErrOverflow
	}
	return Money{Amount: m.Amount + o.Amount, Currency: m.Currency}, nil
}

func (m Money) IsPositive() bool { return m.Amount > 0 }

func (m Money) IsZero() bool { return m.Amount == 0 }

// String formats m for logs, e.g. "1250 GEL" (minor units). Formatting with
// decimals needs the currency's MinorUnits, which live in the database.
func (m Money) String() string {
	return fmt.Sprintf("%d %s", m.Amount, m.Currency)
}
