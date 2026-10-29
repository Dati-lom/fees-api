package money

import "errors"

// CurrencyCode is an ISO 4217 code, e.g. "GEL".
type CurrencyCode string

// Currency is one row of the currencies table, which is the source of truth
// for which currencies are supported. Add a row to support a new currency;
// no code change or restart is needed.
type Currency struct {
	Code       CurrencyCode
	Name       string
	MinorUnits int // decimal places: 2 means 1250 = 12.50
}

var ErrUnsupportedCurrency = errors.New("money: unsupported currency")
