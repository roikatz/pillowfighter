// Package generator produces realistic e-commerce order documents for load testing,
// with an optional padding field to hit a target approximate document size.
package generator

import (
	"encoding/json"
	"fmt"
	"time"

	"github.com/brianvoe/gofakeit/v7"
)

// Order is the business document written and read by the load generator. It models
// a realistic e-commerce order with nested customer, line-item, shipping, and
// payment details, matching the shape a real Couchbase KV workload would store.
type Order struct {
	Type      string    `json:"type"` // always "order"
	OrderID   string    `json:"orderId"`
	CreatedAt time.Time `json:"createdAt"`
	UpdatedAt time.Time `json:"updatedAt"` // always >= CreatedAt
	Status    string    `json:"status"`
	Customer  Customer  `json:"customer"`
	Items     []Item    `json:"items"`
	Shipping  Shipping  `json:"shipping"`
	Payment   Payment   `json:"payment"`
	Totals    Totals    `json:"totals"`
	// Padding pads the encoded document toward a target size; empty when unused.
	Padding string `json:"padding,omitempty"`
}

type Customer struct {
	ID    string `json:"id"`
	Name  string `json:"name"`
	Email string `json:"email"`
	Tier  string `json:"tier"` // BRONZE / SILVER / GOLD / PLATINUM
}

type Item struct {
	SKU       string  `json:"sku"`
	Name      string  `json:"name"`
	Qty       int     `json:"qty"`
	UnitPrice float64 `json:"unitPrice"`
}

type Address struct {
	Street  string `json:"street"`
	City    string `json:"city"`
	State   string `json:"state"`
	Zip     string `json:"zip"`
	Country string `json:"country"`
}

type Shipping struct {
	Method  string  `json:"method"` // STANDARD / EXPRESS / OVERNIGHT
	Cost    float64 `json:"cost"`
	Address Address `json:"address"`
}

type Payment struct {
	Method string `json:"method"` // CREDIT_CARD / PAYPAL / GIFT_CARD
	Status string `json:"status"` // AUTHORIZED / CAPTURED / FAILED
}

type Totals struct {
	Subtotal float64 `json:"subtotal"`
	Tax      float64 `json:"tax"`
	Total    float64 `json:"total"`
}

// createdAtLookback is how far back an order's CreatedAt may be randomized
// from the current time, giving documents a realistic spread of ages rather
// than every document sharing the run's wall-clock timestamp.
const createdAtLookback = 365 * 24 * time.Hour

var (
	statuses        = []string{"CONFIRMED", "PROCESSING", "SHIPPED", "DELIVERED", "CANCELLED"}
	tiers           = []string{"BRONZE", "SILVER", "GOLD", "PLATINUM"}
	shippingMethods = []string{"STANDARD", "EXPRESS", "OVERNIGHT"}
	paymentMethods  = []string{"CREDIT_CARD", "PAYPAL", "GIFT_CARD"}
	paymentStatuses = []string{"AUTHORIZED", "CAPTURED", "FAILED"}
)

// New builds a realistic Order for the given index (used for deterministic key
// derivation elsewhere), padded toward targetSize bytes when targetSize > 0.
func New(index int64, targetSize int) Order {
	itemCount := gofakeit.Number(1, 5)
	items := make([]Item, itemCount)
	var subtotal float64
	for i := range items {
		qty := gofakeit.Number(1, 4)
		price := gofakeit.Price(5, 500)
		items[i] = Item{
			SKU:       gofakeit.UUID(),
			Name:      gofakeit.ProductName(),
			Qty:       qty,
			UnitPrice: price,
		}
		subtotal += price * float64(qty)
	}
	tax := subtotal * 0.08
	shippingCost := gofakeit.Price(0, 25)

	// Spread createdAt over the lookback window, then place updatedAt
	// somewhere between that instant and now so the pair stays coherent.
	now := time.Now()
	createdAt := gofakeit.DateRange(now.Add(-createdAtLookback), now)
	updatedAt := gofakeit.DateRange(createdAt, now)

	order := Order{
		Type:      "order",
		OrderID:   fmt.Sprintf("order-%d-%s", index, gofakeit.UUID()),
		CreatedAt: createdAt,
		UpdatedAt: updatedAt,
		Status:    gofakeit.RandomString(statuses),
		Customer: Customer{
			ID:    gofakeit.UUID(),
			Name:  gofakeit.Name(),
			Email: gofakeit.Email(),
			Tier:  gofakeit.RandomString(tiers),
		},
		Items: items,
		Shipping: Shipping{
			Method: gofakeit.RandomString(shippingMethods),
			Cost:   shippingCost,
			Address: Address{
				Street:  gofakeit.Street(),
				City:    gofakeit.City(),
				State:   gofakeit.State(),
				Zip:     gofakeit.Zip(),
				Country: gofakeit.Country(),
			},
		},
		Payment: Payment{
			Method: gofakeit.RandomString(paymentMethods),
			Status: gofakeit.RandomString(paymentStatuses),
		},
		Totals: Totals{
			Subtotal: round2(subtotal),
			Tax:      round2(tax),
			Total:    round2(subtotal + tax + shippingCost),
		},
	}

	if targetSize > 0 {
		order.Padding = padTo(order, targetSize)
	}

	return order
}

func round2(v float64) float64 {
	return float64(int64(v*100+0.5)) / 100
}

// padTo returns a filler string sized so that the JSON-encoded document (with the
// padding field included) approximates targetSize bytes. Returns "" if the
// unpadded document already meets or exceeds targetSize.
func padTo(o Order, targetSize int) string {
	base, err := jsonSize(o)
	if err != nil || base >= targetSize {
		return ""
	}
	// Account for the `"padding":""` field overhead already counted in base.
	deficit := targetSize - base
	if deficit <= 0 {
		return ""
	}
	return gofakeit.LetterN(uint(deficit))
}

func jsonSize(o Order) (int, error) {
	b, err := json.Marshal(o)
	if err != nil {
		return 0, err
	}
	return len(b), nil
}
