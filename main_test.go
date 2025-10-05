// main_test.go
package main

import (
	"io"
	"math"
	"testing"
	"time"

	log "github.com/sirupsen/logrus"
)

func init() {
	// Disable logs during tests to avoid noise
	log.SetOutput(io.Discard)
}

// -------------------- Tests pour buildPriceMap --------------------

func TestBuildPriceMap(t *testing.T) {
	t.Run("single price per content", func(t *testing.T) {
		prices := []ContentPriceRow{
			{ContentPriceID: 1, ContentID: 1, Price: 10.0, InsertDate: time.Now()},
			{ContentPriceID: 2, ContentID: 2, Price: 20.0, InsertDate: time.Now()},
		}
		result := buildPriceMap(prices)

		if len(result) != 2 {
			t.Errorf("expected 2 prices, got %d", len(result))
		}
		if result[1] != 10.0 {
			t.Errorf("ContentID 1: expected 10.0, got %v", result[1])
		}
		if result[2] != 20.0 {
			t.Errorf("ContentID 2: expected 20.0, got %v", result[2])
		}
	})

	t.Run("multiple prices - keeps latest InsertDate", func(t *testing.T) {
		now := time.Now()
		prices := []ContentPriceRow{
			{ContentID: 1, Price: 10.0, InsertDate: now.Add(-24 * time.Hour)},
			{ContentID: 1, Price: 15.0, InsertDate: now},
			{ContentID: 1, Price: 12.0, InsertDate: now.Add(-48 * time.Hour)},
		}
		result := buildPriceMap(prices)

		if len(result) != 1 {
			t.Errorf("expected 1 content, got %d", len(result))
		}
		if result[1] != 15.0 {
			t.Errorf("expected latest price 15.0, got %v", result[1])
		}
	})

	t.Run("empty input", func(t *testing.T) {
		result := buildPriceMap([]ContentPriceRow{})
		if len(result) != 0 {
			t.Errorf("expected empty map, got %d entries", len(result))
		}
	})
}

// -------------------- Tests pour buildEmailMap --------------------

func TestBuildEmailMap(t *testing.T) {
	t.Run("single email per customer", func(t *testing.T) {
		now := time.Now()
		data := []CustomerDataRow{
			{CustomerID: 100, ChannelValue: "test1@example.com", InsertDate: now},
			{CustomerID: 101, ChannelValue: "test2@example.com", InsertDate: now},
		}
		result := buildEmailMap(data)

		if len(result) != 2 {
			t.Errorf("expected 2 emails, got %d", len(result))
		}
		if result[100] != "test1@example.com" {
			t.Errorf("CustomerID 100: expected test1@example.com, got %s", result[100])
		}
	})

	t.Run("multiple emails - keeps latest InsertDate", func(t *testing.T) {
		now := time.Now()
		data := []CustomerDataRow{
			{CustomerID: 100, ChannelValue: "old@example.com", InsertDate: now.Add(-24 * time.Hour)},
			{CustomerID: 100, ChannelValue: "new@example.com", InsertDate: now},
		}
		result := buildEmailMap(data)

		if len(result) != 1 {
			t.Errorf("expected 1 customer, got %d", len(result))
		}
		if result[100] != "new@example.com" {
			t.Errorf("expected latest email new@example.com, got %s", result[100])
		}
	})

	t.Run("empty input", func(t *testing.T) {
		result := buildEmailMap([]CustomerDataRow{})
		if len(result) != 0 {
			t.Errorf("expected empty map, got %d entries", len(result))
		}
	})
}

// -------------------- Tests pour computeCA --------------------

func TestComputeCA(t *testing.T) {
	t.Run("basic CA calculation", func(t *testing.T) {
		events := []EventRow{
			{EventDataID: 1, ContentID: 10, CustomerID: 100, Quantity: 2},
			{EventDataID: 2, ContentID: 11, CustomerID: 100, Quantity: 1},
			{EventDataID: 3, ContentID: 10, CustomerID: 101, Quantity: 1},
		}

		priceMap := map[int]float64{
			10: 9.99,
			11: 5.00,
		}

		ca := computeCA(events, priceMap)

		if len(ca) != 2 {
			t.Fatalf("expected 2 customers, got %d", len(ca))
		}

		wantCustomer100 := 2*9.99 + 1*5.00
		if !floatEqual(ca[100], wantCustomer100, 0.001) {
			t.Errorf("customer 100 CA: got %.2f, want %.2f", ca[100], wantCustomer100)
		}

		wantCustomer101 := 9.99
		if !floatEqual(ca[101], wantCustomer101, 0.001) {
			t.Errorf("customer 101 CA: got %.2f, want %.2f", ca[101], wantCustomer101)
		}
	})

	t.Run("missing price - event ignored", func(t *testing.T) {
		events := []EventRow{
			{EventDataID: 1, ContentID: 10, CustomerID: 100, Quantity: 2},
			{EventDataID: 2, ContentID: 99, CustomerID: 100, Quantity: 5},
		}

		priceMap := map[int]float64{
			10: 10.0,
		}

		ca := computeCA(events, priceMap)

		want := 2 * 10.0
		if !floatEqual(ca[100], want, 0.001) {
			t.Errorf("customer 100 CA: got %.2f, want %.2f (missing price should be ignored)", ca[100], want)
		}
	})

	t.Run("multiple events same customer", func(t *testing.T) {
		events := []EventRow{
			{ContentID: 10, CustomerID: 100, Quantity: 1},
			{ContentID: 10, CustomerID: 100, Quantity: 2},
			{ContentID: 11, CustomerID: 100, Quantity: 1},
		}

		priceMap := map[int]float64{
			10: 10.0,
			11: 5.0,
		}

		ca := computeCA(events, priceMap)

		want := 1*10.0 + 2*10.0 + 1*5.0
		if !floatEqual(ca[100], want, 0.001) {
			t.Errorf("customer 100 CA: got %.2f, want %.2f", ca[100], want)
		}
	})

	t.Run("zero quantity", func(t *testing.T) {
		events := []EventRow{
			{ContentID: 10, CustomerID: 100, Quantity: 0},
		}

		priceMap := map[int]float64{
			10: 10.0,
		}

		ca := computeCA(events, priceMap)

		if ca[100] != 0.0 {
			t.Errorf("customer 100 CA: got %.2f, want 0.0", ca[100])
		}
	})

	t.Run("empty events", func(t *testing.T) {
		ca := computeCA([]EventRow{}, map[int]float64{10: 10.0})
		if len(ca) != 0 {
			t.Errorf("expected empty CA map, got %d entries", len(ca))
		}
	})

	t.Run("empty price map", func(t *testing.T) {
		events := []EventRow{
			{ContentID: 10, CustomerID: 100, Quantity: 1},
		}
		ca := computeCA(events, map[int]float64{})

		if len(ca) != 0 {
			t.Errorf("expected empty CA map (all prices missing), got %d entries", len(ca))
		}
	})
}

// -------------------- Tests pour mapToSortedSlice --------------------

func TestMapToSortedSlice(t *testing.T) {
	t.Run("sorts descending by CA", func(t *testing.T) {
		caMap := map[int64]float64{
			100: 50.0,
			101: 100.0,
			102: 25.0,
		}
		emailMap := map[int64]string{
			100: "user100@test.com",
			101: "user101@test.com",
			102: "user102@test.com",
		}

		result := mapToSortedSlice(caMap, emailMap)

		if len(result) != 3 {
			t.Fatalf("expected 3 customers, got %d", len(result))
		}

		if result[0].CustomerID != 101 || !floatEqual(result[0].CA, 100.0, 0.001) {
			t.Errorf("position 0: expected CustomerID=101 CA=100.0, got CustomerID=%d CA=%.2f",
				result[0].CustomerID, result[0].CA)
		}
		if result[1].CustomerID != 100 || !floatEqual(result[1].CA, 50.0, 0.001) {
			t.Errorf("position 1: expected CustomerID=100 CA=50.0, got CustomerID=%d CA=%.2f",
				result[1].CustomerID, result[1].CA)
		}
		if result[2].CustomerID != 102 || !floatEqual(result[2].CA, 25.0, 0.001) {
			t.Errorf("position 2: expected CustomerID=102 CA=25.0, got CustomerID=%d CA=%.2f",
				result[2].CustomerID, result[2].CA)
		}
	})

	t.Run("includes emails", func(t *testing.T) {
		caMap := map[int64]float64{100: 50.0}
		emailMap := map[int64]string{100: "test@example.com"}

		result := mapToSortedSlice(caMap, emailMap)

		if result[0].Email != "test@example.com" {
			t.Errorf("expected email test@example.com, got %s", result[0].Email)
		}
	})

	t.Run("missing email", func(t *testing.T) {
		caMap := map[int64]float64{100: 50.0}
		emailMap := map[int64]string{}

		result := mapToSortedSlice(caMap, emailMap)

		if result[0].Email != "" {
			t.Errorf("expected empty email, got %s", result[0].Email)
		}
	})

	t.Run("empty input", func(t *testing.T) {
		result := mapToSortedSlice(map[int64]float64{}, map[int64]string{})
		if len(result) != 0 {
			t.Errorf("expected empty slice, got %d entries", len(result))
		}
	})
}

// -------------------- Tests pour computeQuantiles --------------------

func TestComputeQuantiles(t *testing.T) {
	t.Run("basic quantile calculation", func(t *testing.T) {
		sorted := []CustomerCA{
			{CustomerID: 100, CA: 100.0},
			{CustomerID: 101, CA: 90.0},
			{CustomerID: 102, CA: 80.0},
			{CustomerID: 103, CA: 70.0},
			{CustomerID: 104, CA: 60.0},
			{CustomerID: 105, CA: 50.0},
			{CustomerID: 106, CA: 40.0},
			{CustomerID: 107, CA: 30.0},
			{CustomerID: 108, CA: 20.0},
			{CustomerID: 109, CA: 10.0},
		}

		qStats, top := computeQuantiles(sorted, 0.25)

		if len(qStats) != 4 {
			t.Errorf("expected 4 quantiles, got %d", len(qStats))
		}

		if qStats[0].NbClients != 3 {
			t.Errorf("quantile 0: expected 3 clients, got %d", qStats[0].NbClients)
		}
		if !floatEqual(qStats[0].MaxCA, 100.0, 0.001) {
			t.Errorf("quantile 0 MaxCA: expected 100.0, got %.2f", qStats[0].MaxCA)
		}
		if !floatEqual(qStats[0].MinCA, 80.0, 0.001) {
			t.Errorf("quantile 0 MinCA: expected 80.0, got %.2f", qStats[0].MinCA)
		}

		if len(top) != 3 {
			t.Errorf("expected 3 top customers, got %d", len(top))
		}
		if top[0].CustomerID != 100 {
			t.Errorf("top[0]: expected CustomerID=100, got %d", top[0].CustomerID)
		}
	})

	t.Run("quantile 0.025 (2.5%)", func(t *testing.T) {
		sorted := make([]CustomerCA, 100)
		for i := 0; i < 100; i++ {
			sorted[i] = CustomerCA{
				CustomerID: int64(i),
				CA:         float64(100 - i),
			}
		}

		qStats, top := computeQuantiles(sorted, 0.025)

		if len(qStats) != 40 {
			t.Errorf("expected 40 quantiles, got %d", len(qStats))
		}

		if qStats[0].NbClients != 3 {
			t.Errorf("quantile 0: expected 3 clients, got %d", qStats[0].NbClients)
		}

		if len(top) != 3 {
			t.Errorf("expected 3 top customers, got %d", len(top))
		}
		if !floatEqual(top[0].CA, 100.0, 0.001) {
			t.Errorf("top customer CA: expected 100.0, got %.2f", top[0].CA)
		}
	})

	t.Run("single customer", func(t *testing.T) {
		sorted := []CustomerCA{{CustomerID: 100, CA: 50.0}}
		qStats, top := computeQuantiles(sorted, 0.5)

		if len(qStats) != 2 {
			t.Errorf("expected 2 quantiles, got %d", len(qStats))
		}
		if len(top) != 1 {
			t.Errorf("expected 1 top customer, got %d", len(top))
		}
	})

	t.Run("empty input", func(t *testing.T) {
		qStats, top := computeQuantiles([]CustomerCA{}, 0.25)
		if qStats != nil {
			t.Errorf("expected nil qStats, got %d entries", len(qStats))
		}
		if top != nil {
			t.Errorf("expected nil top, got %d entries", len(top))
		}
	})

	t.Run("quantile edge case - exact division", func(t *testing.T) {
		sorted := []CustomerCA{
			{CustomerID: 100, CA: 100.0},
			{CustomerID: 101, CA: 80.0},
			{CustomerID: 102, CA: 60.0},
			{CustomerID: 103, CA: 40.0},
		}

		qStats, top := computeQuantiles(sorted, 0.5)

		if len(qStats) != 2 {
			t.Errorf("expected 2 quantiles, got %d", len(qStats))
		}
		if qStats[0].NbClients != 2 {
			t.Errorf("quantile 0: expected 2 clients, got %d", qStats[0].NbClients)
		}
		if qStats[1].NbClients != 2 {
			t.Errorf("quantile 1: expected 2 clients, got %d", qStats[1].NbClients)
		}
		if len(top) != 2 {
			t.Errorf("expected 2 top customers, got %d", len(top))
		}
	})
}

// -------------------- Helper functions --------------------

func floatEqual(a, b, epsilon float64) bool {
	return math.Abs(a-b) < epsilon
}

// -------------------- Benchmarks --------------------

func BenchmarkComputeCA(b *testing.B) {
	events := make([]EventRow, 10000)
	for i := 0; i < 10000; i++ {
		events[i] = EventRow{
			ContentID:  i % 100,
			CustomerID: int64(i % 1000),
			Quantity:   1,
		}
	}

	priceMap := make(map[int]float64)
	for i := 0; i < 100; i++ {
		priceMap[i] = float64(i) * 1.5
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = computeCA(events, priceMap)
	}
}

func BenchmarkBuildPriceMap(b *testing.B) {
	prices := make([]ContentPriceRow, 1000)
	now := time.Now()
	for i := 0; i < 1000; i++ {
		prices[i] = ContentPriceRow{
			ContentID:  i % 100,
			Price:      float64(i) * 1.5,
			InsertDate: now.Add(time.Duration(i) * time.Second),
		}
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = buildPriceMap(prices)
	}
}

func BenchmarkMapToSortedSlice(b *testing.B) {
	caMap := make(map[int64]float64)
	emailMap := make(map[int64]string)
	for i := 0; i < 1000; i++ {
		caMap[int64(i)] = float64(i) * 2.5
		emailMap[int64(i)] = "test@example.com"
	}

	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		_ = mapToSortedSlice(caMap, emailMap)
	}
}
