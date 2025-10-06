// main.go
//
// Usage example:
//   go run main.go -quantile=0.025 -since=2020-04-01
//
// This program follows Load -> Compute in Memory -> Export, with no SQL JOINs.
// It reads CustomerEventData (type 6 since 2020-04-01), ContentPrice, CustomerData(email),
// computes CA per customer, generates quantiles and exports top quantile to MySQL table.

package main

import (
	"database/sql"
	"flag"
	"fmt"
	"math"
	"math/rand"
	"os"
	"sort"
	"strings"
	"time"

	_ "github.com/go-sql-driver/mysql"
	"github.com/schollz/progressbar/v3"
	log "github.com/sirupsen/logrus"
)

// -------------------- Data structures --------------------

type EventRow struct {
	EventDataID int64
	EventID     int64
	ContentID   int
	CustomerID  int64
	EventTypeID int
	EventDate   time.Time
	Quantity    int
	InsertDate  time.Time
}

type ContentPriceRow struct {
	ContentPriceID int64
	ContentID      int
	Price          float64
	Currency       string
	InsertDate     time.Time
}

type CustomerDataRow struct {
	CustomerChannelID int64
	CustomerID        int64
	ChannelTypeID     int
	ChannelValue      string
	InsertDate        time.Time
}

type CustomerCA struct {
	CustomerID int64
	Email      string
	CA         float64
}

// Quantile stats
type QuantileStats struct {
	MinCA     float64
	MaxCA     float64
	NbClients int
}

// -------------------- Globals / config --------------------

var (
	quantile  = 0.025
	sinceStr  = "2020-04-01"
	verbose   = false
	batchSize = 500
)

// -------------------- Utility / logging --------------------

func init() {
	// logger setup
	log.SetFormatter(&log.TextFormatter{
		FullTimestamp:   true,
		TimestampFormat: time.RFC3339,
	})
	log.SetOutput(os.Stderr)
	if verbose || strings.ToLower(os.Getenv("VERBOSE")) == "true" {
		log.SetLevel(log.DebugLevel)
	} else {
		log.SetLevel(log.InfoLevel)
	}
}

// helper to read env vars
func env(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}

func mustParseDate(d string) time.Time {
	t, err := time.Parse("2006-01-02", d)
	if err != nil {
		log.Fatalf("invalid date %s: %v", d, err)
	}
	return t
}

// -------------------- LOAD phase --------------------

func openDB() (*sql.DB, error) {
	user := env("DB_USER", "")
	pass := env("DB_PASS", "")
	host := env("DB_HOST", "127.0.0.1")
	port := env("DB_PORT", "3306")
	dbname := env("DB_NAME", "")

	if user == "" || pass == "" || dbname == "" {
		return nil, fmt.Errorf("DB credentials missing; set DB_USER, DB_PASS and DB_NAME environment variables")
	}

	dsn := fmt.Sprintf("%s:%s@tcp(%s:%s)/%s?parseTime=true&loc=Local", user, pass, host, port, dbname)
	db, err := sql.Open("mysql", dsn)
	if err != nil {
		return nil, err
	}
	// reasonable limits
	db.SetMaxOpenConns(10)
	db.SetMaxIdleConns(5)
	db.SetConnMaxLifetime(time.Minute * 5)
	return db, nil
}

// Read events (no joins): EventTypeID = 6, EventDate >= since
func loadEvents(db *sql.DB, since time.Time) ([]EventRow, error) {
	log.WithFields(log.Fields{"stage": "LOAD", "table": "CustomerEventData"}).Info("loading events")
	q := `SELECT EventDataID, EventID, ContentID, CustomerID, EventTypeID, EventDate, Quantity, InsertDate
	      FROM CustomerEventData
	      WHERE EventTypeID = ? AND EventDate >= ?`
	rows, err := db.Query(q, 6, since)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []EventRow
	for rows.Next() {
		var r EventRow
		if err := rows.Scan(&r.EventDataID, &r.EventID, &r.ContentID, &r.CustomerID, &r.EventTypeID, &r.EventDate, &r.Quantity, &r.InsertDate); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	log.WithFields(log.Fields{"loaded_events": len(out)}).Info("events loaded")
	return out, nil
}

// Read content prices (no joins). We will choose latest InsertDate per ContentID in memory.
func loadContentPrices(db *sql.DB) ([]ContentPriceRow, error) {
	log.WithField("stage", "LOAD").Info("loading content prices")
	q := `SELECT ContentPriceID, ContentID, Price, Currency, InsertDate FROM ContentPrice`
	rows, err := db.Query(q)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []ContentPriceRow
	for rows.Next() {
		var r ContentPriceRow
		if err := rows.Scan(&r.ContentPriceID, &r.ContentID, &r.Price, &r.Currency, &r.InsertDate); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	log.WithField("loaded_prices", len(out)).Info("content prices loaded")
	return out, nil
}

// Read customer emails (CustomerData with ChannelTypeID = 1)
func loadCustomerEmails(db *sql.DB) ([]CustomerDataRow, error) {
	log.WithField("stage", "LOAD").Info("loading customer emails (CustomerData channel=1)")
	q := `SELECT CustomerChannelID, CustomerID, ChannelTypeID, ChannelValue, InsertDate FROM CustomerData WHERE ChannelTypeID = ?`
	rows, err := db.Query(q, 1)
	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var out []CustomerDataRow
	for rows.Next() {
		var r CustomerDataRow
		if err := rows.Scan(&r.CustomerChannelID, &r.CustomerID, &r.ChannelTypeID, &r.ChannelValue, &r.InsertDate); err != nil {
			return nil, err
		}
		out = append(out, r)
	}
	log.WithField("loaded_emails", len(out)).Info("customer emails loaded")
	return out, nil
}

// -------------------- COMPUTE phase --------------------

// Build price map: choose latest InsertDate per ContentID
func buildPriceMap(prices []ContentPriceRow) map[int]float64 {
	priceMap := make(map[int]ContentPriceRow)
	for _, p := range prices {
		ex, ok := priceMap[p.ContentID]
		if !ok || p.InsertDate.After(ex.InsertDate) {
			priceMap[p.ContentID] = p
		}
	}
	out := make(map[int]float64, len(priceMap))
	for k, v := range priceMap {
		out[k] = v.Price
	}
	return out
}

// Build email map: choose latest InsertDate per CustomerID
func buildEmailMap(cd []CustomerDataRow) map[int64]string {
	m := make(map[int64]CustomerDataRow)
	for _, r := range cd {
		ex, ok := m[r.CustomerID]
		if !ok || r.InsertDate.After(ex.InsertDate) {
			m[r.CustomerID] = r
		}
	}
	out := make(map[int64]string, len(m))
	for k, v := range m {
		out[k] = v.ChannelValue
	}
	return out
}

// Compute CA per customer given events and price map
func computeCA(events []EventRow, priceMap map[int]float64) map[int64]float64 {
	ca := make(map[int64]float64)
	missingPrices := make(map[int]int) // ContentID -> count of events with missing price

	// progress bar
	bar := progressbar.Default(int64(len(events)), "computing CA")
	for _, e := range events {
		if err := bar.Add(1); err != nil {
			log.Warnf("progress bar error: %v", err)
		}

		price, ok := priceMap[e.ContentID]
		if !ok {
			// missing price -> track and skip
			missingPrices[e.ContentID]++
			if log.IsLevelEnabled(log.DebugLevel) {
				log.WithFields(log.Fields{
					"content_id":  e.ContentID,
					"eventdataid": e.EventDataID,
				}).Debug("missing price for content; skipping")
			}
			continue
		}
		ca[e.CustomerID] += price * float64(e.Quantity)
	}

	if len(missingPrices) > 0 {
		totalSkipped := 0
		for _, count := range missingPrices {
			totalSkipped += count
		}
		log.WithFields(log.Fields{
			"unique_content_ids":   len(missingPrices),
			"total_events_skipped": totalSkipped,
			"percentage_skipped":   fmt.Sprintf("%.2f%%", float64(totalSkipped)/float64(len(events))*100),
		}).Warn("missing prices detected")

		// Log détail si verbose
		if log.IsLevelEnabled(log.DebugLevel) {
			log.Debug("missing price details:")
			for contentID, count := range missingPrices {
				log.Debugf("  ContentID %d: %d events skipped", contentID, count)
			}
		}
	} else {
		log.Info("all events had corresponding prices")
	}

	return ca
}

func mapToSortedSlice(caMap map[int64]float64, emailMap map[int64]string) []CustomerCA {
	out := make([]CustomerCA, 0, len(caMap))
	for cid, v := range caMap {
		email := emailMap[cid]
		out = append(out, CustomerCA{
			CustomerID: cid,
			Email:      email,
			CA:         v,
		})
	}
	// sort descending by CA
	sort.Slice(out, func(i, j int) bool { return out[i].CA > out[j].CA })
	return out
}

// print 10 random samples from map
func printRandomSamples(caMap map[int64]float64, n int) {
	total := len(caMap)
	if total == 0 {
		log.Info("no CA entries to sample")
		return
	}
	log.Infof("printing %d random samples from CA map (total=%d)", n, total)
	ids := make([]int64, 0, total)
	for k := range caMap {
		ids = append(ids, k)
	}
	rand.Seed(time.Now().UnixNano())
	for i := 0; i < n && i < total; i++ {
		idx := rand.Intn(len(ids))
		cid := ids[idx]
		log.Infof("sample %d: CustomerID=%d CA=%.2f", i+1, cid, caMap[cid])
	}
}

// compute quantiles and stats
func computeQuantiles(sorted []CustomerCA, quantile float64) (map[int]QuantileStats, []CustomerCA) {
	n := len(sorted)
	if n == 0 {
		return nil, nil
	}
	qCount := int(math.Round(1.0 / quantile))
	if qCount <= 0 {
		qCount = 1
	}

	// size per quantile (ceil to distribute all)
	size := int(math.Ceil(float64(n) / float64(qCount)))
	qstats := make(map[int]QuantileStats, qCount)

	for i := 0; i < qCount; i++ {
		start := i * size
		end := start + size
		if start >= n {
			// empty bucket
			qstats[i] = QuantileStats{MinCA: 0, MaxCA: 0, NbClients: 0}
			continue
		}
		if end > n {
			end = n
		}
		bucket := sorted[start:end]
		min := bucket[len(bucket)-1].CA
		max := bucket[0].CA
		qstats[i] = QuantileStats{MinCA: min, MaxCA: max, NbClients: len(bucket)}
	}

	// first quantile (top quantile) = index 0 because sorted descending
	topN := size
	if topN > n {
		topN = n
	}
	top := sorted[0:topN]
	return qstats, top
}

// -------------------- EXPORT phase --------------------

// create table if not exists
func ensureExportTable(db *sql.DB, tableName string) error {
	q := fmt.Sprintf(`CREATE TABLE IF NOT EXISTS %s (
	CustomerID BIGINT NOT NULL PRIMARY KEY,
	Email VARCHAR(255),
	CA DECIMAL(18,2) NOT NULL
) ENGINE=InnoDB;`, tableName)
	_, err := db.Exec(q)
	return err
}

// batch insert (mass insert) with ON DUPLICATE KEY UPDATE
func exportTopCustomers(db *sql.DB, tableName string, top []CustomerCA) error {
	if len(top) == 0 {
		log.Info("no top customers to export")
		return nil
	}
	log.WithFields(log.Fields{"stage": "EXPORT", "table": tableName, "count": len(top)}).Info("exporting top customers (batch)")

	// prepare batches
	type batchRow struct {
		cid   int64
		email string
		ca    float64
	}
	rows := make([]batchRow, 0, len(top))
	for _, r := range top {
		rows = append(rows, batchRow{cid: r.CustomerID, email: r.Email, ca: r.CA})
	}

	bar := progressbar.Default(int64(len(rows)), "exporting batches")

	for i := 0; i < len(rows); i += batchSize {
		end := i + batchSize
		if end > len(rows) {
			end = len(rows)
		}
		sub := rows[i:end]

		// build query
		vals := make([]string, 0, len(sub))
		args := make([]interface{}, 0, len(sub)*3)
		for _, r := range sub {
			vals = append(vals, "(?, ?, ?)")
			args = append(args, r.cid, r.email, fmt.Sprintf("%.2f", r.ca))
		}
		q := fmt.Sprintf("INSERT INTO %s (CustomerID, Email, CA) VALUES %s ON DUPLICATE KEY UPDATE Email=VALUES(Email), CA=VALUES(CA)",
			tableName, strings.Join(vals, ","))
		// exec
		tx, err := db.Begin()
		if err != nil {
			return err
		}
		if _, err := tx.Exec(q, args...); err != nil {
			tx.Rollback()
			return err
		}
		if err := tx.Commit(); err != nil {
			return err
		}

		if err := bar.Add(len(sub)); err != nil {
			log.Warnf("progress bar error: %v", err)
		}
	}
	return nil
}

// -------------------- Main --------------------

func main() {
	// Define and parse flags in main() to avoid conflicts with test flags
	flag.Float64Var(&quantile, "quantile", 0.025, "quantile fraction (ex: 0.025)")
	flag.StringVar(&sinceStr, "since", "2020-04-01", "EventDate lower bound (YYYY-MM-DD)")
	flag.BoolVar(&verbose, "verbose", false, "verbose logging")
	flag.Parse()

	// Update log level after parsing flags
	if verbose || strings.ToLower(os.Getenv("VERBOSE")) == "true" {
		log.SetLevel(log.DebugLevel)
	}

	start := time.Now()
	log.WithField("stage", "START").Infof("starting process. quantile=%v since=%s", quantile, sinceStr)

	// parse date
	since := mustParseDate(sinceStr)

	// open DB
	db, err := openDB()
	if err != nil {
		log.Fatalf("db open error: %v", err)
	}
	defer db.Close()

	// LOAD
	events, err := loadEvents(db, since)
	if err != nil {
		log.Fatalf("failed to load events: %v", err)
	}
	prices, err := loadContentPrices(db)
	if err != nil {
		log.Fatalf("failed to load content prices: %v", err)
	}
	emails, err := loadCustomerEmails(db)
	if err != nil {
		log.Fatalf("failed to load customer emails: %v", err)
	}

	// COMPUTE
	priceMap := buildPriceMap(prices)
	log.WithField("price_map_size", len(priceMap)).Info("price map built")
	emailMap := buildEmailMap(emails)
	log.WithField("email_map_size", len(emailMap)).Info("email map built")

	caMap := computeCA(events, priceMap)
	log.WithField("customers_with_ca", len(caMap)).Info("computed CA per customer")

	// Debug: Log CA for specific customers mentioned in the issue
	if log.IsLevelEnabled(log.DebugLevel) {
		testCustomers := []int64{46, 114, 237, 417, 10, 933, 836}
		for _, cid := range testCustomers {
			if ca, ok := caMap[cid]; ok {
				log.Debugf("Customer %d CA: %.2f", cid, ca)
			}
		}
	}

	// print 10 samples
	printRandomSamples(caMap, 10)

	// sorted slice
	sorted := mapToSortedSlice(caMap, emailMap)

	// quantiles
	qStats, top := computeQuantiles(sorted, quantile)
	if qStats == nil {
		log.Warn("no quantile stats (no customers)")
	} else {
		log.Info("========== QUANTILE ANALYSIS ==========")
		for i := 0; i < len(qStats); i++ {
			s := qStats[i]
			startPct := float64(i) * quantile * 100
			endPct := float64(i+1) * quantile * 100

			log.WithFields(log.Fields{
				"quantile_index": i,
				"quantile_range": fmt.Sprintf("%.1f%% - %.1f%%", startPct, endPct),
				"nb_clients":     s.NbClients,
				"min_ca":         fmt.Sprintf("%.2f", s.MinCA),
				"max_ca":         fmt.Sprintf("%.2f", s.MaxCA),
				"avg_ca":         fmt.Sprintf("%.2f", (s.MinCA+s.MaxCA)/2),
			}).Info("quantile summary")
		}
		log.Info("=======================================")
		log.WithField("top_quantile_size", len(top)).Info("top quantile extracted")
	}

	// EXPORT
	dateSuffix := time.Now().Format("20060102")
	tableName := fmt.Sprintf("test_export_%s", dateSuffix)
	if err := ensureExportTable(db, tableName); err != nil {
		log.Fatalf("failed to ensure export table: %v", err)
	}
	if err := exportTopCustomers(db, tableName, top); err != nil {
		log.Fatalf("failed to export top customers: %v", err)
	}

	elapsed := time.Since(start)
	log.WithField("duration", elapsed.String()).Info("process finished")
}
