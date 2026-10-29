package money

import (
	"errors"
	"fmt"
	"sync/atomic"
)

// CurrencyCode is an ISO 4217 code, e.g. "GEL". It is what other records
type CurrencyCode string

// Currency is one row of the currencies table.
type Currency struct {
	Code       CurrencyCode
	Name       string
	MinorUnits int // decimal places: 2 means 1250 = 12.50
}

var ErrUnsupportedCurrency = errors.New("money: unsupported currency")

// supported holds the currencies loaded from the database at startup.
var supported atomic.Pointer[map[CurrencyCode]Currency]

// SetSupportedCurrencies replaces the set of supported currencies.
// Called once at service startup with the rows from the currencies table.
func SetSupportedCurrencies(currencies []Currency) {
	m := make(map[CurrencyCode]Currency, len(currencies))
	for _, c := range currencies {
		m[c.Code] = c
	}
	supported.Store(&m)
}

// LookupCurrency returns the currency for code, or ErrUnsupportedCurrency.
func LookupCurrency(code CurrencyCode) (Currency, error) {
	if m := supported.Load(); m != nil {
		if c, ok := (*m)[code]; ok {
			return c, nil
		}
	}
	return Currency{}, fmt.Errorf("%w: %q", ErrUnsupportedCurrency, code)
}

func (c CurrencyCode) Validate() error {
	_, err := LookupCurrency(c)
	return err
}
