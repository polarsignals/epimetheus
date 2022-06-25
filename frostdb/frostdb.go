package frostdb

import (
	"context"
	"fmt"
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
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).Eq(logicalplan.Literal(matcher.Value)))
		case labels.MatchNotEqual:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).NotEq(logicalplan.Literal(matcher.Value)))
		case labels.MatchRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexMatch(matcher.Value))
		case labels.MatchNotRegexp:
			exprs = append(exprs, logicalplan.Col("labels."+matcher.Name).RegexNotMatch(matcher.Value))
		}
	}

	records := []arrow.Record{}
	f.table.View(func(tx uint64) error {
		return f.table.Iterator(context.Background(), tx, memory.NewGoAllocator(), nil, logicalplan.And(exprs...), nil, func(ar arrow.Record) error {
			records = append(records, ar)
			fmt.Println("Record: ", ar)
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
			StorageLayout: parquet.Encoded(parquet.String(), &parquet.RLEDictionary),
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

type arrowSeriesSet struct {
	index int
	sets  []series
}

type arrowSeries struct {
	index int
	series
}

func (a *arrowSeriesSet) Next() bool {
	a.index++
	return a.index < len(a.sets)
}

func (a *arrowSeriesSet) At() storage.Series {
	return &arrowSeries{
		index:  -1,
		series: a.sets[a.index],
	}
}

func (a *arrowSeriesSet) Err() error                 { return nil }
func (a *arrowSeriesSet) Warnings() storage.Warnings { return nil }

func (a *arrowSeries) Labels() labels.Labels { return a.l }
func (a *arrowSeries) Iterator() chunkenc.Iterator {
	return a
}

func (a *arrowSeries) Next() bool {
	a.index++
	return a.index < len(a.ts)
}

func (a *arrowSeries) Seek(i int64) bool {
	return false // TODO
}

func (a *arrowSeries) At() (int64, float64) {
	return a.ts[a.index], a.v[a.index]
}

func (a *arrowSeries) Err() error { return nil }

func seriesSetFromRecords(ar []arrow.Record) storage.SeriesSet {

	sets := map[uint64]series{}
	for _, r := range ar {
		seriesset := parseRecord(r)
		for id, set := range seriesset {
			if s, ok := sets[id]; ok {
				s.ts = append(s.ts, set.ts...)
				s.v = append(s.v, set.v...)
			} else {
				sets[id] = set
			}
		}
	}

	// Flatten sets
	ss := []series{}
	for _, s := range sets {
		ss = append(ss, s)
	}

	return &arrowSeriesSet{
		index: -1,
		sets:  ss,
	}
}

type series struct {
	l  labels.Labels
	ts []int64
	v  []float64
}

func parseRecord(r arrow.Record) map[uint64]series {

	seriesset := map[uint64]series{}

	for i := 0; i < r.Column(0).Len(); i++ {
		lbls := labels.Labels{}
		var ts int64
		var v float64
		for j := 0; j < int(r.NumCols()); j++ {
			switch {
			case r.ColumnName(j) == "timestamp":
				ts = r.Column(j).(*array.Int64).Value(i)
			case r.ColumnName(j) == "value":
				v = r.Column(j).(*array.Float64).Value(i)
			default:
				name := strings.TrimPrefix(r.ColumnName(j), "labels")
				value := r.Column(j).(*array.Binary).Value(i)
				lbls = append(lbls, labels.Label{
					Name:  name,
					Value: string(value),
				})
			}
		}
		h := lbls.Hash()
		if es, ok := seriesset[h]; ok {
			es.ts = append(es.ts, ts)
			es.v = append(es.v, v)
		} else {
			seriesset[h] = series{
				ts: []int64{ts},
				v:  []float64{v},
				l:  lbls,
			}
		}
	}

	return seriesset
}
