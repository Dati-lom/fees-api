package payment

import (
	"context"
	"errors"
	"strings"
	"sync"
)

// Account ID prefixes that make MockCharger misbehave, for demos and tests:
//
//	"decline-…" every charge is declined (→ bill ends CHARGE_FAILED)
//	"flaky-…"   the first 2 attempts fail with a transient error, then it
//	            succeeds (shows Temporal retrying the activity)
const (
	DeclinePrefix = "decline-"
	FlakyPrefix   = "flaky-"
	flakyFailures = 2
)

var ErrUnavailable = errors.New("payment: provider temporarily unavailable")

// MockCharger simulates a payment provider in memory. Like a real provider,
// it is idempotent: repeating an IdempotencyKey returns the first result.
type MockCharger struct {
	mu       sync.Mutex
	charges  map[string]ChargeResult // by idempotency key
	attempts map[string]int          // by idempotency key, for "flaky-"
}

func NewMockCharger() *MockCharger {
	return &MockCharger{
		charges:  map[string]ChargeResult{},
		attempts: map[string]int{},
	}
}

func (m *MockCharger) Charge(ctx context.Context, req ChargeRequest) (ChargeResult, error) {
	m.mu.Lock()
	defer m.mu.Unlock()

	if res, ok := m.charges[req.IdempotencyKey]; ok {
		return res, nil
	}

	switch {
	case strings.HasPrefix(req.AccountID, DeclinePrefix):
		return ChargeResult{}, ErrDeclined
	case strings.HasPrefix(req.AccountID, FlakyPrefix):
		m.attempts[req.IdempotencyKey]++
		if m.attempts[req.IdempotencyKey] <= flakyFailures {
			return ChargeResult{}, ErrUnavailable
		}
	}

	res := ChargeResult{ChargeID: "mock_ch_" + req.IdempotencyKey}
	m.charges[req.IdempotencyKey] = res
	return res, nil
}
