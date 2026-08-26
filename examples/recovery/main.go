// Command recovery demonstrates crash recovery: an execution left
// StepStatusRunning by a process that exited mid-run is picked up and
// resumed by a RecoveryManager, continuing from exactly where it left
// off rather than starting over.
//
// IMPORTANT — read before treating this as a template: this example
// uses store/memory purely to demonstrate the recovery *mechanism*.
// memory.Store keeps its data in this same process's memory, so if the
// process actually crashed, memory.Store's data would be wiped out along
// with it — there would be nothing left to recover from. Real crash
// recovery requires a Store backed by something that outlives the
// process (Postgres, Redis, a file on disk, ...); see the README's
// Persistence and Recovery sections. Since nothing here actually
// restarts, the "crash" below is simulated by writing a partially
// progressed Execution directly into the store.
//
// Run it with:
//
//	go run ./examples/recovery
package main

import (
	"context"
	"fmt"
	"time"

	saga "github.com/adnanlari/go-orchestrator"
	"github.com/adnanlari/go-orchestrator/store/memory"
)

func main() {
	store := memory.New()

	workflow := saga.New("order_creation", saga.WithStore(store))
	workflow.AddStep(saga.Step("reserve_inventory", reserveInventory, releaseInventory))
	workflow.AddStep(saga.Step("charge_payment", chargePayment, refundPayment))
	workflow.AddStep(saga.Step("ship_order", shipOrder, cancelShipment))

	// Simulate a process that got partway through an order before
	// exiting: reserve_inventory had already succeeded and been
	// persisted, charge_payment was recorded Running (in flight) at the
	// moment the process died — so it's unknown whether the charge
	// actually went through — and ship_order never started. In a real
	// deployment this record would already be sitting in the Store from
	// an earlier Execute call that never returned; here we seed it
	// directly to simulate that starting point.
	crashed := &saga.Execution{
		ID:       "exec-simulated-crash",
		SagaName: "order_creation",
		Status:   saga.StatusRunning,
		Input:    "ord-99",
		Steps: []saga.StepExecution{
			{Name: "reserve_inventory", Status: saga.StepStatusSucceeded, Output: "ord-99-reserved"},
			{Name: "charge_payment", Status: saga.StepStatusRunning},
			{Name: "ship_order", Status: saga.StepStatusPending},
		},
		CreatedAt: time.Now(),
	}
	if err := store.Save(context.Background(), crashed); err != nil {
		panic(err)
	}
	fmt.Println("--- Simulated a crash: reserve_inventory done, charge_payment in flight, ship_order never started ---")
	fmt.Println()

	rm := saga.NewRecoveryManager(store, saga.WithSaga(workflow))
	results, err := rm.Recover(context.Background())
	if err != nil {
		panic(err)
	}

	fmt.Println()
	for _, r := range results {
		fmt.Printf("recovered execution %s -> status %s\n", r.Execution.ID, r.Execution.Status)
		if r.Err != nil {
			fmt.Printf("  error: %v\n", r.Err)
		}
	}
}

// reserveInventory is never called during recovery above: the engine
// sees it already StepStatusSucceeded in the persisted record and skips
// straight to the step after it.
func reserveInventory(ctx context.Context, data any) (any, error) {
	fmt.Println("  reserve_inventory: ran normally")
	return data, nil
}
func releaseInventory(ctx context.Context, data any) error { return nil }

// chargePayment IS called again here: it was recorded Running (in
// flight) when the simulated crash happened, so the engine has no way to
// know whether the real charge succeeded before the process died. This
// is exactly the scenario OperationID (see examples/payment) exists for
// — a real Action would pass it to the payment gateway so a duplicate
// invocation like this one is safely deduplicated rather than charging
// twice.
func chargePayment(ctx context.Context, data any) (any, error) {
	fmt.Println("  charge_payment: re-invoked by recovery (was in flight when the process exited)")
	return data, nil
}
func refundPayment(ctx context.Context, data any) error {
	fmt.Println("  compensate charge_payment: refunding")
	return nil
}

// shipOrder runs normally: it never started before the crash.
func shipOrder(ctx context.Context, data any) (any, error) {
	fmt.Println("  ship_order: ran normally, never started before the crash")
	return data, nil
}
func cancelShipment(ctx context.Context, data any) error { return nil }
