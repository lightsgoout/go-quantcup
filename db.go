package main

import (
	"database/sql"

	"github.com/lib/pq"
	"log"
	"math/rand"
)

//	{"SYM", "ID0", Bid, 4818, 179},
//  {"SYM", "ID0", Bid, 0, 179},
//
const (
	randomSeed   = 42
	cancelChance = 0.05
)

func ResetSchema(db *sql.DB) {
	schemaDDL := `
		DROP TYPE IF EXISTS exchange_side CASCADE;
		CREATE TYPE exchange_side AS ENUM ('bid', 'ask');
		
		DROP TYPE IF EXISTS symbol CASCADE;
		CREATE TYPE symbol as ENUM ('SYM');
		
		DROP TABLE IF EXISTS orders CASCADE;
		CREATE TABLE orders (
			id serial primary key,
			symbol symbol,
			trader varchar,
			side exchange_side,
			price int,
			size int,
			blocked_size int
		) with (fillfactor=90);
		
		DROP TABLE IF EXISTS deals CASCADE;
		CREATE TABLE deals (
			id serial primary key,
			bid_order_id bigint,
			ask_order_id bigint,
			price int,
			size int,
			symbol symbol
		);
	`

	_, err := db.Exec(schemaDDL)
	if err != nil {
		log.Fatal(err)
	}
	log.Printf("DB schema created")
}

func FillTestData(db *sql.DB, ordersToGenerate int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(pq.CopyIn("orders", "symbol", "trader", "side", "price", "size"))
	if err != nil {
		log.Fatal(err)
	}

	rand.Seed(randomSeed)

	for i := 0; i < ordersToGenerate; i++ {
		var order = GenerateRandomOrder(cancelChance)
		var sqlSide = ""
		if order.side == Bid {
			sqlSide = "bid"
		} else {
			sqlSide = "ask"
		}

		_, err := stmt.Exec(order.symbol, order.trader, sqlSide, order.price, order.size)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("%d random orders generated", ordersToGenerate)

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

}

const fetchOrdersSQL = `
	SELECT 
		id, 
		symbol, 
		trader, 
		case when side = 'bid' then 0 else 1 end as side, 
		price, 
		size - coalesce(blocked_size, 0) as size 
		FROM orders ORDER BY id ASC
	FOR UPDATE NOWAIT
`

func FetchOrders(tx *sql.Tx) []Order {
	rows, err := tx.Query(fetchOrdersSQL)
	if err != nil {
		log.Fatal(err)
	}
	defer rows.Close()

	var result []Order

	for rows.Next() {
		var (
			o Order
		)
		if err := rows.Scan(&o.id, &o.symbol, &o.trader, &o.side, &o.price, &o.size); err != nil {
			log.Fatal(err)
		}
		result = append(result, o)
	}
	return result
}

func min(a, b uint64) uint64 {
	if a < b {
		return a
	}
	return b
}

func max(a, b uint64) uint64 {
	if a > b {
		return a
	}
	return b
}

func PersistDeals(tx *sql.Tx, deals DealSlice) {
	stmt, err := tx.Prepare(pq.CopyIn("deals", "bid_order_id", "ask_order_id", "price", "size", "symbol"))
	if err != nil {
		log.Fatal(err)
	}

	var minOrderID uint64 = 0
	var maxOrderID uint64 = 0

	for _, deal := range deals {
		if minOrderID == 0 {
			minOrderID = min(deal.bidOrderID, deal.askOrderID)
		} else {
			minOrderID = min(min(deal.bidOrderID, deal.askOrderID), minOrderID)
		}

		if maxOrderID == 0 {
			maxOrderID = max(deal.bidOrderID, deal.askOrderID)
		} else {
			maxOrderID = max(max(deal.bidOrderID, deal.askOrderID), maxOrderID)
		}

		_, err := stmt.Exec(deal.bidOrderID, deal.askOrderID, deal.price, deal.size, deal.symbol)
		if err != nil {
			log.Fatal(err)
		}
	}

	log.Printf("%d deals created", len(deals))

	_, err = stmt.Exec()
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	var blockOrdersSql = `
		with cte as (
			select sum(d.size) as blocked_size, d.bid_order_id as order_id from deals d where d.bid_order_id between $1 and $2 group by d.bid_order_id
			union all
			select sum(d.size) as blocked_size, d.ask_order_id as order_id from deals d where d.ask_order_id between $1 and $2 group by d.ask_order_id
		)

		update orders as o
		set blocked_size = cte.blocked_size
		from cte
		where cte.order_id = o.id and (o.id between $1 and $2)
	`

	stmt, err = tx.Prepare(blockOrdersSql)
	_, err = stmt.Exec(minOrderID, maxOrderID)
	if err != nil {
		log.Fatal(err)
	}
	err = stmt.Close()
	if err != nil {
		log.Fatal(err)
	}

	log.Printf("min = %d, max = %d blocked", minOrderID, maxOrderID)
}
