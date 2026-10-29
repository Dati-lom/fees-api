// Package workflow holds the Temporal workflow that owns a bill's lifecycle,
// one workflow execution per bill (the "entity workflow" pattern).
package workflow

import (
	"encore.app/fees/domain"
	"encore.dev/types/uuid"
	"go.temporal.io/sdk/workflow"
)

// TaskQueue is the Temporal task queue the fees worker polls.
const TaskQueue = "fees"

// WorkflowID returns the workflow ID for a bill. Using the bill ID makes
// starting the same bill twice fail, so bill creation is idempotent.
func WorkflowID(billID uuid.UUID) string {
	return "bill-" + billID.String()
}

// BillWorkflow runs for the whole life of one bill: OPEN → CLOSING → CLOSED.
// Skeleton for now: it only creates the bill. Line-item updates, the close
// signal, the period-end timer and the charge step come next.
func BillWorkflow(ctx workflow.Context, params domain.NewBillParams) error {
	_, err := domain.NewBill(params)
	return err
}
