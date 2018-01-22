package main

import (
	"fmt"
	"time"

	"database/sql"
	"github.com/grd/stat"
	"log"
)

const (
	batchSize   int = 10
	replayCount     = 10

	nanoToSeconds    = 1e-9
	ordersToGenerate = 100000
)

func main() {
	var e Engine

	connStr := "user=kolombet dbname=exchange sslmode=disable"
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		log.Fatal(err)
	}

	// batch latency measurements.
	engineLatencies := make([]time.Duration, replayCount*ordersToGenerate)
	fetchLatencies := make([]time.Duration, replayCount)
	persistLatencies := make([]time.Duration, replayCount)
	totalLatencies := make([]time.Duration, replayCount)

	for j := 0; j < replayCount; j++ {
		log.Printf("=== Round #%d", j+1)
		e.Reset(db, 100000)
		totalBegin := time.Now()

		tx, err := db.Begin()
		if err != nil {
			log.Fatal(err)
		}

		fetchBegin := time.Now()
		OrdersFeed := FetchOrders(tx)
		fetchEnd := time.Now()
		fetchLatencies[j] = fetchEnd.Sub(fetchBegin)

		for i := batchSize; i < len(OrdersFeed); i += batchSize {
			begin := time.Now()
			feed(&e, i-batchSize, i, OrdersFeed)
			end := time.Now()
			engineLatencies[i/batchSize-1+(j*(ordersToGenerate/batchSize))] = end.Sub(begin)
		}

		persistBegin := time.Now()
		e.Persist(tx)
		err = tx.Commit()
		if err != nil {
			log.Fatal(err)
		}
		persistEnd := time.Now()
		persistLatencies[j] = persistEnd.Sub(persistBegin)

		totalEnd := time.Now()
		totalLatencies[j] = totalEnd.Sub(totalBegin)
	}

	engineDurations := DurationSlice(engineLatencies)
	fetchDurations := DurationSlice(fetchLatencies)
	persistDurations := DurationSlice(persistLatencies)
	totalDurations := DurationSlice(totalLatencies)

	var mean float64 = stat.Mean(engineDurations)
	var stdDev = stat.SdMean(engineDurations, mean)

	fmt.Printf("[engine] mean(latency) = %1.2f, sd(latency) = %1.2f\n", mean*nanoToSeconds, stdDev*nanoToSeconds)

	var fetchMean = stat.Mean(fetchDurations)
	var fetchStdDev = stat.SdMean(fetchDurations, mean)
	fmt.Printf("[fetch] mean(latency) = %1.2f, sd(latency) = %1.2f\n", fetchMean*nanoToSeconds, fetchStdDev*nanoToSeconds)

	var persistMean = stat.Mean(persistDurations)
	var persistStdDev = stat.SdMean(persistDurations, mean)
	fmt.Printf("[persist] mean(latency) = %1.2f, sd(latency) = %1.2f\n", persistMean*nanoToSeconds, persistStdDev*nanoToSeconds)

	var totalMean = stat.Mean(totalDurations)
	fmt.Printf("[total] %1.1f orders per second", ordersToGenerate/(totalMean*nanoToSeconds))
}

func feed(e *Engine, begin, end int, Orders []Order) {
	for i := begin; i < end; i++ {
		var order Order = Orders[i]
		if order.price != 0 {
			e.Limit(order)
		}
	}
}

type DurationSlice []time.Duration

func (f DurationSlice) Get(i int) float64 { return float64(f[i]) }
func (f DurationSlice) Len() int          { return len(f) }
