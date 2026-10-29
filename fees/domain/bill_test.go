package domain

import (
	"testing"
	"time"

	"encore.app/fees/money"
	"github.com/stretchr/testify/require"
)

func TestNewBillParamsMatches(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	p := NewBillParams{
		AccountID:   "acc-1",
		Currency:    "GEL",
		PeriodStart: start,
		PeriodEnd:   start.AddDate(0, 1, 0),
		Now:         start,
	}
	bill, err := NewBill(p)
	require.NoError(t, err)

	retry := p
	retry.Now = start.Add(time.Hour)
	tbilisi := time.FixedZone("GET", 4*60*60)
	retry.PeriodStart = start.In(tbilisi) // same instant, different zone
	require.True(t, retry.Matches(bill))

	for name, change := range map[string]func(*NewBillParams){
		"account":      func(p *NewBillParams) { p.AccountID = "acc-2" },
		"currency":     func(p *NewBillParams) { p.Currency = "USD" },
		"period start": func(p *NewBillParams) { p.PeriodStart = start.Add(time.Second) },
		"period end":   func(p *NewBillParams) { p.PeriodEnd = p.PeriodEnd.Add(time.Second) },
	} {
		other := p
		change(&other)
		require.False(t, other.Matches(bill), name)
	}
}

func TestRefundCannotMakeTotalNegative(t *testing.T) {
	start := time.Date(2026, 10, 1, 0, 0, 0, 0, time.UTC)
	bill, err := NewBill(NewBillParams{
		AccountID: "acc-1", Currency: "GEL",
		PeriodStart: start, PeriodEnd: start.AddDate(0, 1, 0), Now: start,
	})
	require.NoError(t, err)
	gel := func(amount int64) LineItem {
		return LineItem{Amount: money.Money{Amount: amount, Currency: "GEL"}}
	}

	require.ErrorIs(t, bill.AddLineItem(gel(-1)), ErrNegativeTotal, "refund on an empty bill")
	require.NoError(t, bill.AddLineItem(gel(500)))
	require.ErrorIs(t, bill.AddLineItem(gel(-501)), ErrNegativeTotal)
	require.NoError(t, bill.AddLineItem(gel(-500)), "refunding down to exactly zero is fine")
	require.ErrorIs(t, bill.AddLineItem(gel(-1)), ErrNegativeTotal)
	require.Len(t, bill.LineItems, 2)

	require.NoError(t, bill.StartClosing())
	require.NoError(t, bill.FreezeTotal())
	require.Equal(t, money.Money{Amount: 0, Currency: "GEL"}, *bill.Total)
}
