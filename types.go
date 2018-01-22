package main

import (
	"fmt"
	"math/rand"
)

type Price uint16 // 0-65536 eg the price 123.45 = 12345
type OrderID uint64
type Size uint64
type Side int

type InputOrder struct {
	symbol string
	trader string
	side   Side
	price  Price
	size   Size
}

type Order struct {
	id     uint64
	symbol string
	trader string
	side   Side
	price  Price
	size   Size
}

var traderChoices = []string{"ID0", "ID1", "ID2", "ID3", "ID4", "ID5", "ID6", "ID7", "ID8"}

func GenerateRandomOrder(cancelChance float32) InputOrder {
	var isCancelled = rand.Float32() <= cancelChance
	var price = 0

	if isCancelled {
		price = 0
	} else {
		price = rand.Intn(int(maxPrice) - 1)
	}

	return InputOrder{
		"SYM",
		traderChoices[rand.Intn(9)],
		Side(rand.Intn(2)),
		Price(price),
		Size(rand.Intn(1000)),
	}
}

// Execution Report (send one per opposite-sided order completely filled).
type Execution Order

const (
	Bid Side = iota
	Ask
)

func (o *Execution) String() string {
	return fmt.Sprintf("{symbol: %v, trader: %v, side: %v, price: %v, size: %v}", o.symbol, o.trader, o.side, o.price, o.size)
}

func (o *Order) String() string {
	return fmt.Sprintf("{symbol: %v, trader: %v, side: %v, price: %v, size: %v}", o.symbol, o.trader, o.side, o.price, o.size)
}

func (s Side) String() string {
	switch s {
	case Bid:
		return "Bid"
	default:
		return "Ask"
	}
}
