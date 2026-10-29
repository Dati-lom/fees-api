package fees

import (
	"context"
	"fmt"

	"encore.app/fees/money"
	"encore.app/fees/payment"
	"encore.app/fees/store"
	billworkflow "encore.app/fees/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// CONNECTION CONFIG
const (
	temporalHostPort  = client.DefaultHostPort // localhost:7233
	temporalNamespace = client.DefaultNamespace
)

//encore:service
type Service struct {
	store    *store.Store
	temporal client.Client
	worker   worker.Worker
}

func initService() (*Service, error) {
	ctx := context.Background()
	st := store.New(db)

	// The currencies table is the source of truth; keep it in memory for
	// validation (workflow code can't query the DB).
	currencies, err := st.LoadCurrencies(ctx)
	if err != nil {
		return nil, err
	}
	money.SetSupportedCurrencies(currencies)

	c, err := client.DialContext(ctx, client.Options{
		HostPort:  temporalHostPort,
		Namespace: temporalNamespace,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to temporal: %w", err)
	}

	w := worker.New(c, billworkflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(billworkflow.BillWorkflow)
	w.RegisterActivity(&billworkflow.Activities{
		Store:   st,
		Charger: payment.NewMockCharger(),
	})
	if err := w.Start(); err != nil {
		c.Close()
		return nil, fmt.Errorf("start temporal worker: %w", err)
	}

	return &Service{store: st, temporal: c, worker: w}, nil
}

// Shutdown is called by Encore on graceful shutdown.
func (s *Service) Shutdown(force context.Context) {
	s.worker.Stop()
	s.temporal.Close()
}
