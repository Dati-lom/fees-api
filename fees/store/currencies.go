package store

import (
	"context"
	"errors"
	"fmt"

	"encore.app/fees/money"
	"encore.dev/storage/sqldb"
)

// ListCurrencies returns every row of the currencies table.
func (s *Store) ListCurrencies(ctx context.Context) ([]money.Currency, error) {
	rows, err := s.db.Query(ctx, `SELECT code, name, minor_units FROM currencies ORDER BY code`)
	if err != nil {
		return nil, fmt.Errorf("load currencies: %w", err)
	}
	defer rows.Close()

	var currencies []money.Currency
	for rows.Next() {
		var c money.Currency
		if err := rows.Scan(&c.Code, &c.Name, &c.MinorUnits); err != nil {
			return nil, fmt.Errorf("scan currency: %w", err)
		}
		currencies = append(currencies, c)
	}
	if err := rows.Err(); err != nil {
		return nil, fmt.Errorf("load currencies: %w", err)
	}
	return currencies, nil
}

// GetCurrency returns the currency with code, or money.ErrUnsupportedCurrency
// if there is no such row.
func (s *Store) GetCurrency(ctx context.Context, code money.CurrencyCode) (money.Currency, error) {
	var c money.Currency
	err := s.db.QueryRow(ctx, `
		SELECT code, name, minor_units FROM currencies WHERE code = $1
	`, code).Scan(&c.Code, &c.Name, &c.MinorUnits)
	if errors.Is(err, sqldb.ErrNoRows) {
		return money.Currency{}, fmt.Errorf("%w: %q", money.ErrUnsupportedCurrency, code)
	}
	if err != nil {
		return money.Currency{}, fmt.Errorf("get currency: %w", err)
	}
	return c, nil
}
