package ironflock

import (
	"context"
	"errors"
	"fmt"
	"math"
	"slices"
	"strings"
	"sync"

	"github.com/ironflock/sdk-go/internal/jsontext"
)

// validateReadTable checks the name of a table or transform to read: non-empty
// and without leading or trailing whitespace.
func validateReadTable(table string) error {
	if strings.TrimSpace(table) == "" {
		return invalidf("Tablename must not be empty!")
	}
	if strings.TrimSpace(table) != table {
		return invalidf("Tablename %q cannot have leading/trailing whitespace", table)
	}
	return nil
}

// decodeRows converts the result of a history-style procedure into rows: no
// value reads as no rows, a list of row objects as rows. Anything else is an
// error of operation op.
func decodeRows(op string, res *Result) ([]Row, error) {
	switch v := res.Value().(type) {
	case nil:
		return []Row{}, nil
	case []any:
		rows := make([]Row, len(v))
		for i, e := range v {
			row, ok := e.(map[string]any)
			if !ok {
				return nil, &OperationError{Op: op, Err: fmt.Errorf("%w: row %d is %T, not an object", errUnexpectedResult, i, e)}
			}
			rows[i] = row
		}
		return rows, nil
	default:
		return nil, &OperationError{Op: op, Err: fmt.Errorf("%w %T, want a list of rows", errUnexpectedResult, v)}
	}
}

// chunkedRead is a read of rows that the data backend may answer in chunks.
// fleetdb answers a result that does not fit its message budget (8 MiB of
// JSON) to a caller that asks for progressive results as progressive results
// [chunkIndex, rows], the rows newest first, and a final summary
// {chunked: true, chunkCount, totalRows, order} (DataBackend.respondRows);
// order "asc" means the chunks, concatenated, are the single read's rows in
// reverse. A smaller result is the plain list of rows. A caller that does not
// ask gets sys.dataservice.error.result_too_large instead.
type chunkedRead struct {
	mu     sync.Mutex
	chunks map[int64][]any
	bad    error // the first malformed progressive result
}

// options returns the CallOptions of the read: they ask for progressive
// results and collect them.
func (c *chunkedRead) options() *CallOptions { return &CallOptions{OnProgress: c.add} }

// add collects a progressive result. The connection calls it one result at a
// time, before the call returns.
func (c *chunkedRead) add(res *Result) {
	c.mu.Lock()
	defer c.mu.Unlock()
	if c.bad != nil {
		return
	}
	if res == nil || len(res.Args) != 2 {
		var args []any
		if res != nil {
			args = res.Args
		}
		c.bad = fmt.Errorf("a progressive result is not [chunkIndex, rows]: %s", jsontext.Compact(args))
		return
	}
	i, isIndex := wholeNumber(res.Args[0])
	rows, isList := res.Args[1].([]any)
	_, dup := c.chunks[i]
	switch {
	case !isIndex || i < 0:
		c.bad = fmt.Errorf("a progressive result has the chunk index %s", jsontext.Compact(res.Args[0]))
	case !isList:
		c.bad = fmt.Errorf("chunk %d is %T, not a list of rows", i, res.Args[1])
	case dup:
		c.bad = fmt.Errorf("chunk %d arrived twice", i)
	default:
		if c.chunks == nil {
			c.chunks = make(map[int64][]any)
		}
		c.chunks[i] = rows
	}
}

// rows returns the rows of the read, whose final result is res: the rows the
// result lists, or the chunks reassembled into the rows a single read would
// return, in its order. Errors are errors of operation op.
func (c *chunkedRead) rows(op string, res *Result) ([]Row, error) {
	c.mu.Lock()
	defer c.mu.Unlock()
	fail := func(format string, args ...any) error {
		return &OperationError{Op: op, Err: fmt.Errorf("%w: "+format, append([]any{errUnexpectedResult}, args...)...)}
	}
	if c.bad != nil {
		return nil, fail("%v", c.bad)
	}
	summary, isMap := res.Value().(map[string]any)
	if !isMap || summary["chunked"] != true {
		if len(c.chunks) > 0 {
			return nil, fail("%d chunk(s) of rows arrived, but the result is not their summary: %s",
				len(c.chunks), jsontext.Compact(res.Value()))
		}
		return decodeRows(op, res)
	}
	count, countOK := wholeNumber(summary["chunkCount"])
	total, totalOK := wholeNumber(summary["totalRows"])
	if !countOK || !totalOK || count < 0 || total < 0 {
		return nil, fail("the summary of a chunked result is malformed: %s", jsontext.Compact(summary))
	}
	order, _ := summary["order"].(string)
	if order != "asc" && order != "desc" {
		return nil, fail("a chunked result has the unknown order %s", jsontext.Compact(summary["order"]))
	}
	if int64(len(c.chunks)) != count {
		return nil, fail("a chunked result of %d chunk(s) sent %d", count, len(c.chunks))
	}
	n := 0
	for i := range count {
		chunk, ok := c.chunks[i]
		if !ok {
			return nil, fail("chunk %d of %d did not arrive", i, count)
		}
		n += len(chunk)
	}
	if int64(n) != total {
		return nil, fail("a chunked result of %d row(s) sent %d", total, n)
	}
	rows := make([]Row, 0, n)
	for i := range count {
		for _, e := range c.chunks[i] {
			row, ok := e.(map[string]any)
			if !ok {
				return nil, fail("a row of chunk %d is %T, not an object", i, e)
			}
			rows = append(rows, row)
		}
	}
	if order == "asc" {
		slices.Reverse(rows) // the chunks hold the rows newest first
	}
	return rows, nil
}

// wholeNumber returns v, a number of a payload, as an int64 when it is a
// whole number that fits one.
func wholeNumber(v any) (int64, bool) {
	switch n := v.(type) {
	case int64:
		return n, true
	case uint64:
		return int64(n), n <= math.MaxInt64
	case float64:
		if n == math.Trunc(n) && n >= math.MinInt64 && n < math.MaxInt64 {
			return int64(n), true
		}
	}
	return 0, false
}

// GetHistory reads rows of a table or transform via
// history.transformed.<table>: the newest rows that match q, in ascending
// tsp order (a transform's rows: see TableQueryParams). A nil q reads the 10
// newest rows. Secret columns come back as SecretPlaceholder.
//
// A result too large for one message of the data backend (8 MiB of JSON)
// arrives in chunks, which GetHistory reassembles in memory: the rows are
// those of a single read, in the same order. ctx must cover the whole
// transfer.
//
// The data backend refuses a read with URIResultTooLarge (a row over that
// budget on its own, or a result over its 1 GiB guard), URIInvalidLimit (a
// transform's Limit over 3000) and URISecretColumn (a filter on a secret
// column).
func (f *IronFlock) GetHistory(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if q == nil {
		q = &TableQueryParams{Limit: 10}
	}
	wire, err := queryWire(q, MaxQueryLimit, f.log)
	if err != nil {
		return nil, invalidParams("query", err)
	}
	op := fmt.Sprintf("getHistory('%s')", table)
	topic := "history.transformed." + table
	read := new(chunkedRead)
	res, err := f.call(ctx, topic, []any{wire}, nil, read.options(), f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, false)
	}
	return read.rows(op, res)
}

// GetSeriesHistory reads a table's history down-sampled into time buckets
// via history.transformed.series.<table> (tables only: transforms have no
// series).
//
// Each row is one bucket — of one group, with GroupBy: tsp is the bucket's
// start in epoch milliseconds, each GroupBy column comes back under its own
// name, and each metric under SeriesMetric.Column, e.g.
// row["AVG:temperature"]. The rows ascend by tsp; a bucket without rows is
// absent (no gap filling). A large result is reassembled from chunks as
// GetHistory does.
//
// The data backend refuses what it cannot answer with URIInvalidTimeRange,
// URIInvalidMetric, URIInvalidGroupBy and URISeriesTooManyGroups (more than
// 50,000 rows). Data backends older than fleetdb v1.0.58 do not take this
// query.
func (f *IronFlock) GetSeriesHistory(ctx context.Context, table string, q SeriesQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	wire, err := seriesWire(&q, f.log)
	if err != nil {
		return nil, invalidParams("series query", err)
	}
	op := fmt.Sprintf("getSeriesHistory('%s')", table)
	topic := "history.transformed.series." + table
	read := new(chunkedRead)
	res, err := f.call(ctx, topic, []any{wire}, nil, read.options(), f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, false)
	}
	return read.rows(op, res)
}

// RevealSecrets reads rows of an own table with its secret columns
// decrypted, via secret.reveal.<table>: the newest rows that match q, newest
// first (unlike GetHistory). A nil q reads the 10 most recent rows; Limit
// must be 1-100. Only the app's own containers may call it.
//
// The data backend answers at most 30 calls a minute per app credential on
// a device; beyond that the call fails with URIRateLimited. Reveal a secret
// once and keep it rather than on every use. A filter on a secret column is
// refused with URISecretColumn, a table without secret columns with
// URINotASecretColumn.
func (f *IronFlock) RevealSecrets(ctx context.Context, table string, q *TableQueryParams) ([]Row, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if q == nil {
		q = &TableQueryParams{Limit: 10}
	}
	wire, err := queryWire(q, MaxSecretLimit, f.log)
	if err != nil {
		return nil, invalidParams("query", err)
	}
	op := fmt.Sprintf("revealSecrets('%s')", table)
	topic := "secret.reveal." + table
	read := new(chunkedRead)
	res, err := f.call(ctx, topic, []any{wire}, nil, read.options(), f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, true)
	}
	return read.rows(op, res)
}

// VerifySecret checks candidate against the secret column of the selected
// rows without reading it back (secret.verify.<table>); the comparison runs
// in constant time inside the data backend. A nil q checks the most recent
// row; Limit must be 1-100. A response of an unexpected shape never reads
// as a match.
//
// The data backend answers at most 120 calls a minute per app credential on
// a device; beyond that the call fails with URIRateLimited — for a check on
// every request (an API key, say), cache a positive outcome briefly. A
// column that is not a secret column is refused with URINotASecretColumn,
// a filter on a secret column with URISecretColumn.
func (f *IronFlock) VerifySecret(ctx context.Context, table, column, candidate string, q *TableQueryParams) (*SecretVerifyResult, error) {
	if err := validateReadTable(table); err != nil {
		return nil, err
	}
	if strings.TrimSpace(column) == "" {
		return nil, invalidf("Invalid verify parameters: column must be a non-empty string")
	}
	if q == nil {
		q = &TableQueryParams{Limit: 1}
	}
	wire, err := queryWire(q, MaxSecretLimit, f.log)
	if err != nil {
		return nil, invalidParams("verify", err)
	}
	// column and candidate travel in the same object as the row selection.
	wire["column"] = column
	wire["candidate"] = candidate

	op := fmt.Sprintf("verifySecret('%s')", table)
	topic := "secret.verify." + table
	res, err := f.call(ctx, topic, []any{wire}, nil, nil, f.reconnectWindow)
	if err != nil {
		return nil, historyFailed(op, topic, err, true)
	}
	return verifyResult(res), nil
}

// verifyResult reads the {match, checked} answer of secret.verify. It fails
// closed: only an object whose "match" is the boolean true is a match.
func verifyResult(res *Result) *SecretVerifyResult {
	out := &SecretVerifyResult{}
	m, ok := res.Value().(map[string]any)
	if !ok {
		return out
	}
	out.Match = m["match"] == true
	switch n := m["checked"].(type) {
	case int64:
		out.Checked = int(n)
	case uint64:
		out.Checked = int(min(n, uint64(math.MaxInt)))
	case float64:
		if n >= 0 && n <= math.MaxInt {
			out.Checked = int(n)
		}
	}
	return out
}

// errUnexpectedResult is the cause of decoding failures of service results.
var errUnexpectedResult = errors.New("unexpected result type")
