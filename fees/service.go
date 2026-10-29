package fees

import (
	"context"
	"fmt"

	"encore.app/fees/money"
	billworkflow "encore.app/fees/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// CONNECTION CONFIG
const (
	temporalHostPort  = client.DefaultHostPort // localhost:7233
	temporalNamespace = client.DefaultNamespace
)

type Service struct {
	temporal client.Client
	worker   worker.Worker
}

func initService() (*Service, error) {
	ctx := context.Background()

	if err := loadCurrencies(ctx); err != nil {
		return nil, err
	}

	c, err := client.DialContext(ctx, client.Options{
		HostPort:  temporalHostPort,
		Namespace: temporalNamespace,
	})
	if err != nil {
		return nil, fmt.Errorf("connect to temporal: %w", err)
	}

	w := worker.New(c, billworkflow.TaskQueue, worker.Options{})
	w.RegisterWorkflow(billworkflow.BillWorkflow)
	if err := w.Start(); err != nil {
		c.Close()
		return nil, fmt.Errorf("start temporal worker: %w", err)
	}

	return &Service{temporal: c, worker: w}, nil
}

// Shutdown is called by Encore on graceful shutdown.
func (s *Service) Shutdown(force context.Context) {
	s.worker.Stop()
	s.temporal.Close()
}

// loadCurrencies moved currencies to DB so we just load in var for better usage
func loadCurrencies(ctx context.Context) error {
	rows, err := db.Query(ctx, `SELECT code, name, minor_units FROM currencies`)
	if err != nil {
		return fmt.Errorf("load currencies: %w", err)
	}
	defer rows.Close()

	var currencies []money.Currency
	for rows.Next() {
		var c money.Currency
		if err := rows.Scan(&c.Code, &c.Name, &c.MinorUnits); err != nil {
			return fmt.Errorf("scan currency: %w", err)
		}
		currencies = append(currencies, c)
	}
	if err := rows.Err(); err != nil {
		return fmt.Errorf("load currencies: %w", err)
	}
	money.SetSupportedCurrencies(currencies)
	return nil
}
