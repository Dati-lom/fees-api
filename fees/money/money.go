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

// New returns a Money of amount minor units, validating the currency.
func New(amount int64, c CurrencyCode) (Money, error) {
	if err := c.Validate(); err != nil {
		return Money{}, err
	}
	return Money{Amount: amount, Currency: c}, nil
}

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

// String formats m for humans, e.g. "12.50 GEL". Not for persistence.
func (m Money) String() string {
	cur, err := LookupCurrency(m.Currency)
	if err != nil || cur.MinorUnits == 0 {
		return fmt.Sprintf("%d %s", m.Amount, m.Currency)
	}
	sign := ""
	abs := uint64(m.Amount)
	if m.Amount < 0 {
		sign = "-"
		abs = uint64(-(m.Amount + 1)) + 1 // safe for MinInt64
	}
	div := uint64(math.Pow10(cur.MinorUnits))
	return fmt.Sprintf("%s%d.%0*d %s", sign, abs/div, cur.MinorUnits, abs%div, m.Currency)
}
