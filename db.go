package main

import (
	"database/sql"

	"log"
	"github.com/lib/pq"
	"math/rand"
)

//	{"SYM", "ID0", Bid, 4818, 179},
//  {"SYM", "ID0", Bid, 0, 179},
//

func ResetSchema(db *sql.DB) {
	schemaDDL := `
		DROP TYPE IF EXISTS exchange_side CASCADE;
		CREATE TYPE exchange_side AS ENUM ('bid', 'ask');
		
		DROP TYPE IF EXISTS symbol CASCADE;
		CREATE TYPE symbol as ENUM ('SYM');
		
		DROP TABLE IF EXISTS orders;
		CREATE TABLE orders (
			id serial primary key,
			symbol symbol,
			trader varchar,
			side exchange_side,
			price int,
			size int
		);
		
		DROP TABLE IF EXISTS deals;
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

func FillTestData(db *sql.DB, additionalRandomRecords int) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(pq.CopyIn("orders", "symbol", "trader", "side", "price", "size"))
	if err != nil {
		log.Fatal(err)
	}

	for _, order := range inputOrdersFeed {
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

	rand.Seed(42)

	for i := 0; i <= additionalRandomRecords; i++ {
		var order = GenerateRandomOrder()
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

	log.Printf("%d test orders created", len(inputOrdersFeed))
	log.Printf("%d additional random orders generated", additionalRandomRecords)

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

func FetchOrders(db *sql.DB) []Order {
	rows, err := db.Query("SELECT id, symbol, trader, case when side = 'bid' then 0 else 1 end as side, price, size FROM orders ORDER BY id ASC")
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

func PersistDeals(db *sql.DB, deals DealSlice) {
	tx, err := db.Begin()
	if err != nil {
		log.Fatal(err)
	}

	stmt, err := tx.Prepare(pq.CopyIn("deals", "bid_order_id", "ask_order_id", "price", "size", "symbol"))
	if err != nil {
		log.Fatal(err)
	}

	for _, deal := range deals {
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
	err = tx.Commit()
	if err != nil {
		log.Fatal(err)
	}

}