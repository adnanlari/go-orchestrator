// Command payment demonstrates the features that matter for running
// go-orchestrator in production, together: persistence (Store), retries
// (RetryPolicy, NonRetryable), a step timeout, idempotency (OperationID)
// used against a mock payment gateway that behaves like a real one
// (transient failures, and deduplication on a repeated idempotency key),
// and observability (EventPublisher, Logger).
//
// Run it with:
//
//	go run ./examples/payment
package main

import (
	"context"
	"errors"
	"fmt"
	"log/slog"
	"os"
	"sync"
	"time"

	saga "github.com/adnanlari/go-orchestrator"
	"github.com/adnanlari/go-orchestrator/store/memory"
)

// Order is the data that flows through the saga.
type Order struct {
	ID     string
	SKU    string
	Qty    int
	Amount float64
}

// paymentGateway stands in for a real downstream payment provider. It
// fails the first attempt for any given idempotency key (simulating a
// transient network error, which is what triggers the saga's retry) and
// returns the same charge for a key it has already processed
// (simulating the deduplication a real gateway performs) — which is
// exactly what makes retrying the charge safe.
type paymentGateway struct {
	mu       sync.Mutex
	attempts map[string]int
	charged  map[string]string // idempotency key -> charge ID
}

func newPaymentGateway() *paymentGateway {
	return &paymentGateway{attempts: map[string]int{}, charged: map[string]string{}}
}

func (g *paymentGateway) Charge(idempotencyKey string, amount float64) (string, error) {
	g.mu.Lock()
	defer g.mu.Unlock()

	if chargeID, ok := g.charged[idempotencyKey]; ok {
		fmt.Printf("  [gateway] key %s already processed -> returning original charge %s (no double charge)\n", idempotencyKey, chargeID)
		return chargeID, nil
	}

	g.attempts[idempotencyKey]++
	if g.attempts[idempotencyKey] == 1 {
		fmt.Printf("  [gateway] transient failure for key %s (attempt 1)\n", idempotencyKey)
		return "", errors.New("gateway timeout")
	}

	chargeID := fmt.Sprintf("chg-%d", len(g.charged)+1)
	g.charged[idempotencyKey] = chargeID
	fmt.Printf("  [gateway] charged $%.2f for key %s -> charge %s\n", amount, idempotencyKey, chargeID)
	return chargeID, nil
}

// loggingPublisher prints every lifecycle Event. A real implementation
// might instead push to a metrics counter or an alerting system.
type loggingPublisher struct{}

func (loggingPublisher) Publish(ctx context.Context, ev saga.Event) {
	fmt.Printf("  [event] %s step=%q\n", ev.Type, ev.Step)
}

func main() {
	logger := slog.New(slog.NewTextHandler(os.Stdout, &slog.HandlerOptions{Level: slog.LevelInfo}))
	store := memory.New()
	gateway := newPaymentGateway()

	workflow := saga.New("order_creation",
		saga.WithStore(store),
		saga.WithRetryPolicy(saga.FixedDelay(50*time.Millisecond, 3)),
		saga.WithLogger(logger),
		saga.WithEventPublisher(loggingPublisher{}),
	)
	workflow.AddStep(saga.Step("validate_order", validateOrder, nil)) // nothing to undo
	workflow.AddStep(saga.Step("reserve_inventory", reserveInventory, releaseInventory))
	workflow.AddStep(saga.Step("charge_payment", chargeStep(gateway), refundPayment,
		saga.WithStepTimeout(2*time.Second),
	))

	order := Order{ID: "ord-42", SKU: "WIDGET", Qty: 3, Amount: 59.97}
	fmt.Println("--- Running an order (payment fails once transiently, then retries and succeeds) ---")
	exec, err := workflow.Execute(context.Background(), order)
	fmt.Println()
	fmt.Printf("execution %s finished with status %s\n", exec.ID, exec.Status)
	if err != nil {
		fmt.Printf("error: %v\n", err)
	}
}

func validateOrder(ctx context.Context, data any) (any, error) {
	order := data.(Order)
	if order.Qty <= 0 {
		// A validation failure will never succeed no matter how many
		// times it's retried, so mark it explicitly non-retryable.
		return nil, saga.NonRetryable(fmt.Errorf("invalid quantity: %d", order.Qty))
	}
	fmt.Printf("  validate_order: order %s looks valid\n", order.ID)
	return order, nil
}

func reserveInventory(ctx context.Context, data any) (any, error) {
	order := data.(Order)
	fmt.Printf("  reserve_inventory: reserved %d x %s\n", order.Qty, order.SKU)
	return order, nil
}

func releaseInventory(ctx context.Context, data any) error {
	order := data.(Order)
	fmt.Printf("  compensate reserve_inventory: releasing %d x %s\n", order.Qty, order.SKU)
	return nil
}

func chargeStep(gateway *paymentGateway) saga.ActionFunc {
	return func(ctx context.Context, data any) (any, error) {
		order := data.(Order)
		// OperationID is stable across every retry of this step for this
		// execution, so the gateway sees the same idempotency key on
		// attempt 1 and attempt 2, and can safely deduplicate rather than
		// charging the card twice.
		chargeID, err := gateway.Charge(saga.OperationID(ctx), order.Amount)
		if err != nil {
			return nil, err
		}
		fmt.Printf("  charge_payment: charge ID %s\n", chargeID)
		return order, nil
	}
}

func refundPayment(ctx context.Context, data any) error {
	fmt.Println("  compensate charge_payment: issuing refund")
	return nil
}
