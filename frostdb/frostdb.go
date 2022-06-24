package frostdb

import (
	"context"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	frost "github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/segmentio/parquet-go"
)

type FrostDB struct {
	schema *dynparquet.Schema
	store  *frostdb.ColumnStore
	table  *frostdb.Table
}

type FrostAppender struct {
	ctx      context.Context
	schema   *dynparquet.Schema
	tableRef *frostdb.Table
}

type FrostQuerier struct {
	*FrostDB
}

// Open a new frostDB
func Open(reg prometheus.Registerer, logger log.Logger) (*FrostDB, error) {
	store := frost.New(
		reg,
		8192,
		10*1024*1024,
	)
	db, _ := store.DB("prometheus")
	schema := promSchema()
	table, err := db.Table(
		"metrics",
		frost.NewTableConfig(schema),
		logger,
	)
	if err != nil {
		return nil, err
	}
	return &FrostDB{
		store:  store,
		schema: schema,
		table:  table,
	}, nil
}

func (f *FrostDB) Query() error {
	return nil
}

func (f *FrostQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (f *FrostQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	return nil, nil, nil
}

func (f *FrostQuerier) Close() error { return nil }

func (f *FrostQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {

	exprs := []logicalplan.Expr{
		logicalplan.Col("timestamp").GT(logicalplan.Literal(hints.Start)),
		logicalplan.Col("timestamp").LT(logicalplan.Literal(hints.End)),
	}
	// Build a filter from matchers
	for _, matcher := range matchers {
		switch matcher.Type {
		case labels.MatchEqual:
			exprs = append(exprs, logicalplan.Col(matcher.Name).Eq(logicalplan.Literal(matcher.Value)))
		case labels.MatchNotEqual:
			exprs = append(exprs, logicalplan.Col(matcher.Name).NotEq(logicalplan.Literal(matcher.Value)))
		case labels.MatchRegexp:
			exprs = append(exprs, logicalplan.Col(matcher.Name).RegexMatch(matcher.Value))
		case labels.MatchNotRegexp:
			exprs = append(exprs, logicalplan.Col(matcher.Name).RegexNotMatch(matcher.Value))
		}
	}

	records := []arrow.Record{}
	f.table.View(func(tx uint64) error {
		return f.table.Iterator(context.Background(), tx, memory.NewGoAllocator(), nil, logicalplan.And(exprs...), nil, func(ar arrow.Record) error {
			records = append(records, ar)
			ar.Retain() // retain so we can use them outside of this function
			return nil
		})
	})
	return seriesSetFromRecords(records)
}

// Querier implements the storage.Queryable interface
func (f *FrostDB) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return &FrostQuerier{f}, nil
}

func (f *FrostDB) StartTime() (int64, error) {
	return 0, nil
}

func (f *FrostDB) Close() error {
	return nil
}

func (f *FrostDB) Appender(ctx context.Context) storage.Appender {
	return &FrostAppender{
		ctx:      ctx,
		schema:   f.schema,
		tableRef: f.table,
	}
}

// Append writes immediately to the frostdb. Rollback and Commit are nop
func (f *FrostAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	dynamicColumnNames := make([]string, 0, len(l))
	row := make([]parquet.Value, 0, len(l))
	for i, k := range l {
		dynamicColumnNames = append(dynamicColumnNames, k.Name)
		row = append(row, parquet.ValueOf(k.Value).Level(0, 1, i))
	}

	buf, err := f.schema.NewBuffer(map[string][]string{
		"labels": dynamicColumnNames,
	})
	if err != nil {
		return 0, err
	}

	// Add timestamp and value to row
	row = append(row, parquet.ValueOf(t).Level(0, 0, len(l)))
	row = append(row, parquet.ValueOf(v).Level(0, 0, len(l)+1))

	_, err = buf.WriteRows([]parquet.Row{row})
	if err != nil {
		return 0, err
	}

	_, err = f.tableRef.InsertBuffer(f.ctx, buf)
	if err != nil {
		return 0, err
	}

	return 0, nil
}

func (f *FrostAppender) Rollback() error { return nil }
func (f *FrostAppender) Commit() error   { return nil }
func (f *FrostAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

func (f *FrostDB) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, nil
}

func promSchema() *dynparquet.Schema {
	return dynparquet.NewSchema(
		"metrics_schema",
		[]dynparquet.ColumnDefinition{{
			Name:          "labels",
			StorageLayout: parquet.Encoded(parquet.Optional(parquet.String()), &parquet.RLEDictionary),
			Dynamic:       true,
		}, {
			Name:          "timestamp",
			StorageLayout: parquet.Int(64),
			Dynamic:       false,
		}, {
			Name:          "value",
			StorageLayout: parquet.Leaf(parquet.DoubleType),
			Dynamic:       false,
		}},
		[]dynparquet.SortingColumn{
			dynparquet.NullsFirst(dynparquet.Ascending("labels")),
			dynparquet.Ascending("timestamp"),
		},
	)
}

// TODO: converting arrow records into a series set somehow....
// a series set is multiple different series.
// but arrow records is a single series
type arrowSeriesSet struct {
	recordIdx int
	colIdx    int
	records   []arrow.Record
	l         labels.Labels
}

type arrowSeries struct {
	*arrowSeriesSet
}

func (a *arrowSeriesSet) Next() bool {
	r := a.records[a.recordIdx]
	if a.colIdx+1 != int(r.NumCols()) {
		a.colIdx++
		return true
	}

	// exhausted columns in current record
	a.colIdx = 0
	if a.recordIdx == len(a.records) {
		return false
	}
	a.records[a.recordIdx].Release()
	a.recordIdx++
	return true
}

func (a *arrowSeries) Labels() labels.Labels {

	l := labels.Labels{}
	r := a.records[a.recordIdx]
	for i := int(0); i < int(r.NumCols()); i++ {
		if !strings.HasPrefix(r.ColumnName(i), "labels") {
			return l
		}

		// Build labels
		name := strings.TrimPrefix(r.ColumnName(i), "labels")
		value := r.Column(i).(*array.String).Value(a.colIdx)
		l = append(l, labels.Label{
			Name:  name,
			Value: value,
		})
	}

	return l
}

// TODO
func (a *arrowSeries) Iterator() chunkenc.Iterator {
	return a
}

func (a *arrowSeries) Next() bool {
	a.colIdx++
	// handle colIdx rollover
	r := a.records[a.recordIdx]
	if a.colIdx == int(r.NumCols()) {
		a.colIdx = 0
		a.recordIdx++
		if a.recordIdx == len(a.records) {
			return false
		}
	}

	// TODO we aren't handling label changes between rows

	return true
}

func (a *arrowSeries) Seek(i int64) bool {
	return false // TODO
}

func (a *arrowSeries) At() (int64, float64) {
	var ts int64
	var v float64
	r := a.records[a.recordIdx]
	for i := int(0); i < int(r.NumCols()); i++ {
		switch {
		case strings.HasPrefix(r.ColumnName(i), "value"):
			v = r.Column(i).(*array.Float64).Value(a.colIdx)
		case strings.HasPrefix(r.ColumnName(i), "timestamp"):
			ts = r.Column(i).(*array.Int64).Value(a.colIdx)
		}
	}

	return ts, v
}

func (a *arrowSeries) Err() error { return nil }

func (a *arrowSeriesSet) At() storage.Series {
	return &arrowSeries{a}
}

func (a *arrowSeriesSet) Err() error                 { return nil }
func (a *arrowSeriesSet) Warnings() storage.Warnings { return nil }
func seriesSetFromRecords(ar []arrow.Record) storage.SeriesSet {
	return &arrowSeriesSet{
		records: ar,
	}
}
