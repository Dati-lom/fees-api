package store

import (
	"context"
	"fmt"

	"encore.app/fees/money"
)

// LoadCurrencies returns every row of the currencies table.
func (s *Store) LoadCurrencies(ctx context.Context) ([]money.Currency, error) {
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
