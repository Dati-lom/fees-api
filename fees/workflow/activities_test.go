package workflow

import (
	"context"
	"fmt"
	"testing"

	"encore.app/fees/store"
	"encore.dev/storage/sqldb"
	"github.com/stretchr/testify/require"
)

func TestClassify(t *testing.T) {
	ctx := context.Background()
	for _, tc := range []struct {
		name string
		err  error
		want string
	}{
		{"not found", store.ErrNotFound, ErrTypeNotFound},
		{"bill not open", fmt.Errorf("wrapped: %w", store.ErrBillNotOpen), ErrTypeBillNotOpen},
		{"invalid transition", store.ErrInvalidTransition, ErrTypeInvalidTransition},
		{"total mismatch", store.ErrTotalMismatch, ErrTypeTotalMismatch},
		{"foreign key violation", fmt.Errorf("insert line item: %w", &sqldb.Error{DatabaseCode: "23503"}), ErrTypeDataIntegrity},
		{"check violation", &sqldb.Error{DatabaseCode: "23514"}, ErrTypeDataIntegrity},
		{"invalid input", &sqldb.Error{DatabaseCode: "22P02"}, ErrTypeDataIntegrity},
		{"undefined column", &sqldb.Error{DatabaseCode: "42703"}, ErrTypeDataIntegrity},
	} {
		t.Run(tc.name, func(t *testing.T) {
			err := classify(ctx, tc.err)
			require.Equal(t, tc.want, errType(err))
		})
	}
	require.NoError(t, classify(ctx, nil))
}
