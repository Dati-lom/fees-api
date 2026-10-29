package fees

import (
	"context"
	"fmt"
	"time"

	"encore.app/fees/payment"
	"encore.app/fees/store"
	billworkflow "encore.app/fees/workflow"
	"go.temporal.io/sdk/client"
	"go.temporal.io/sdk/worker"
)

// workerStopTimeout is how long a stopping worker lets in-flight activities
// finish. Encore may force shutdown sooner; see Shutdown.
const workerStopTimeout = 10 * time.Second

// Service runs the API and, in the same process, the Temporal worker that
// executes bill workflows and activities. Nothing ties the two together
// except the task queue, so the worker can move to its own deployment and
// scale separately when needed: start it from a separate binary with the
// same registrations, and drop it from here.
//
//encore:service
type Service struct {
	store    *store.Store
	temporal client.Client
	worker   worker.Worker
}

func initService() (*Service, error) {
	ctx := context.Background()
	st := store.New(db)

	opts := client.Options{
		HostPort:  cfg.TemporalHostPort,
		Namespace: cfg.TemporalNamespace,
	}
	if secrets.TemporalAPIKey != "" {
		opts.Credentials = client.NewAPIKeyStaticCredentials(secrets.TemporalAPIKey)
	}
	c, err := client.DialContext(ctx, opts)
	if err != nil {
		return nil, fmt.Errorf("connect to temporal: %w", err)
	}

	w := worker.New(c, billworkflow.TaskQueue, worker.Options{
		WorkerStopTimeout: workerStopTimeout,
	})
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

// Shutdown is called by Encore on graceful shutdown. It stops the worker,
// letting in-flight activities finish, but returns as soon as force is
// cancelled. Activities cut off then are retried by Temporal on another
// worker; they are all idempotent.
func (s *Service) Shutdown(force context.Context) {
	done := make(chan struct{})
	go func() {
		defer close(done)
		s.worker.Stop()
		s.temporal.Close()
	}()
	select {
	case <-done:
	case <-force.Done():
	}
}
