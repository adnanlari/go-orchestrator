// Command basic demonstrates the minimal core of go-orchestrator:
// defining a saga, running it successfully, and running it again with a
// failure that triggers reverse-order compensation. It uses none of the
// library's optional features — no Store, no retries, no timeouts, no
// observability — see examples/payment for those.
//
// Run it with:
//
//	go run ./examples/basic
package main

import (
	"context"
	"errors"
	"fmt"

	saga "github.com/adnanlari/go-orchestrator"
)

// Order is the data that flows through the saga: each step reads it,
// enriches it, and passes it on to the next step's Action.
type Order struct {
	ID            string
	SKU           string
	Qty           int
	ReservationID string
	ChargeID      string
	PaymentFails  bool // simulates a downstream failure, for this example only
}

func main() {
	workflow := saga.New("order_creation")
	workflow.AddStep(saga.Step("reserve_inventory", reserveInventory, releaseInventory))
	workflow.AddStep(saga.Step("charge_payment", chargePayment, refundPayment))

	fmt.Println("--- Running an order that succeeds ---")
	run(workflow, Order{ID: "ord-1", SKU: "WIDGET", Qty: 2})

	fmt.Println()
	fmt.Println("--- Running an order whose payment fails ---")
	run(workflow, Order{ID: "ord-2", SKU: "WIDGET", Qty: 1, PaymentFails: true})
}

func run(workflow *saga.Definition, order Order) {
	exec, err := workflow.Execute(context.Background(), order)
	fmt.Printf("execution %s finished with status %s\n", exec.ID, exec.Status)
	if err != nil {
		fmt.Printf("  error: %v\n", err)
		return
	}
	fmt.Printf("  result: %+v\n", exec.Output)
}

func reserveInventory(ctx context.Context, data any) (any, error) {
	order := data.(Order)
	order.ReservationID = "res-" + order.ID
	fmt.Printf("  reserve_inventory: reserved %d x %s (reservation %s)\n", order.Qty, order.SKU, order.ReservationID)
	return order, nil
}

func releaseInventory(ctx context.Context, data any) error {
	order := data.(Order)
	fmt.Printf("  compensate reserve_inventory: releasing reservation %s\n", order.ReservationID)
	return nil
}

func chargePayment(ctx context.Context, data any) (any, error) {
	order := data.(Order)
	if order.PaymentFails {
		return nil, errors.New("card declined")
	}
	order.ChargeID = "chg-" + order.ID
	fmt.Printf("  charge_payment: charged order %s (charge %s)\n", order.ID, order.ChargeID)
	return order, nil
}

func refundPayment(ctx context.Context, data any) error {
	order := data.(Order)
	fmt.Printf("  compensate charge_payment: refunding charge %s\n", order.ChargeID)
	return nil
}
