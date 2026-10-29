package payment

import (
	"context"
	"testing"

	"encore.app/fees/money"
	"encore.dev/types/uuid"
	"github.com/stretchr/testify/require"
)

func request(t *testing.T, accountID, key string) ChargeRequest {
	t.Helper()
	id, err := uuid.NewV4()
	require.NoError(t, err)
	return ChargeRequest{
		BillID:         id,
		AccountID:      accountID,
		Amount:         money.Money{Amount: 500, Currency: "GEL"},
		IdempotencyKey: key,
	}
}

func TestRepeatedKeyChargesOnce(t *testing.T) {
	ctx := context.Background()
	m := NewMockCharger()
	req := request(t, "acc-1", "bill-1-charge-1")

	first, err := m.Charge(ctx, req)
	require.NoError(t, err)
	again, err := m.Charge(ctx, req)
	require.NoError(t, err)

	require.Equal(t, first.ChargeID, again.ChargeID, "a retry returns the original charge")
	require.Equal(t, 1, m.Charges())

	_, err = m.Charge(ctx, request(t, "acc-1", "bill-1-charge-2"))
	require.NoError(t, err)
	require.Equal(t, 2, m.Charges(), "a new key is a new charge")
}

func TestFlakyAccountSucceedsAfterRetries(t *testing.T) {
	ctx := context.Background()
	m := NewMockCharger()
	req := request(t, FlakyPrefix+"acc-1", "bill-1-charge-1")

	for range flakyFailures {
		_, err := m.Charge(ctx, req)
		require.ErrorIs(t, err, ErrUnavailable)
	}
	_, err := m.Charge(ctx, req)
	require.NoError(t, err)
	require.Equal(t, 1, m.Charges())
}

func TestDeclinedAccountIsNeverCharged(t *testing.T) {
	m := NewMockCharger()
	_, err := m.Charge(context.Background(), request(t, DeclinePrefix+"acc-1", "bill-1-charge-1"))
	require.ErrorIs(t, err, ErrDeclined)
	require.Equal(t, 0, m.Charges())
}
