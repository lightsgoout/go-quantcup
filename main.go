package main

import (
	"fmt"
	"time"

	"github.com/grd/stat"
	"database/sql"
	"log"
)

const (
	batchSize   int = 10
	replayCount     = 10

	nanoToSeconds   = 1e-9
)

func main() {
	var e Engine

	connStr := "user=kolombet dbname=exchange sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// batch latency measurements.
	engineLatencies := make([]time.Duration, replayCount*(len(inputOrdersFeed)))
	fetchLatencies := make([]time.Duration, replayCount)
	persistLatencies := make([]time.Duration, replayCount)

	for j := 0; j < replayCount; j++ {
		log.Printf("=== Round #%d", j + 1)
		e.Reset(db, 100000)

		fetchBegin := time.Now()
		OrdersFeed := FetchOrders(db)
		fetchEnd := time.Now()
		fetchLatencies[j] = fetchEnd.Sub(fetchBegin)

		for i := batchSize; i < len(OrdersFeed); i += batchSize {
			begin := time.Now()
			feed(&e, i-batchSize, i, OrdersFeed)
			end := time.Now()
			engineLatencies[i/batchSize-1+(j*(len(inputOrdersFeed)/batchSize))] = end.Sub(begin)
		}

		persistBegin := time.Now()
		e.Persist(db)
		persistEnd := time.Now()
		persistLatencies[j] = persistEnd.Sub(persistBegin)
	}

	engineDurations := DurationSlice(engineLatencies)
	fetchDurations := DurationSlice(fetchLatencies)
	persistDurations := DurationSlice(persistLatencies)

	var mean float64 = stat.Mean(engineDurations)
	var stdDev = stat.SdMean(engineDurations, mean)

	fmt.Printf("[engine] mean(latency) = %1.2f, sd(latency) = %1.2f\n", mean * nanoToSeconds, stdDev * nanoToSeconds)

	var fetchMean = stat.Mean(fetchDurations)
	var fetchStdDev = stat.SdMean(fetchDurations, mean)
	fmt.Printf("[fetch] mean(latency) = %1.2f, sd(latency) = %1.2f\n", fetchMean * nanoToSeconds, fetchStdDev * nanoToSeconds)

	var persistMean = stat.Mean(persistDurations)
	var persistStdDev = stat.SdMean(persistDurations, mean)
	fmt.Printf("[persist] mean(latency) = %1.2f, sd(latency) = %1.2f\n", persistMean * nanoToSeconds, persistStdDev * nanoToSeconds)

}

func feed(e *Engine, begin, end int, Orders []Order) {
	for i := begin; i < end; i++ {
		var order Order = Orders[i]
		if order.price == 0 {
			orderID := OrderID(order.size)
			e.Cancel(orderID)
		} else {
			e.Limit(order)
		}
	}
}

type DurationSlice []time.Duration

func (f DurationSlice) Get(i int) float64 { return float64(f[i]) }
func (f DurationSlice) Len() int          { return len(f) }
