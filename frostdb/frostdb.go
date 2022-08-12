package frostdb

import (
	"context"
	"fmt"
	"math"
	"strings"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	frost "github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/segmentio/parquet-go"
)

type FrostDB struct {
	db     *frostdb.DB
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
func Open(dir string, reg prometheus.Registerer, logger log.Logger) (*FrostDB, error) {
	ctx := context.Background()
	store, err := frost.New(
		logger,
		reg,
		frost.WithWAL(),
		frost.WithStoragePath(dir),
	)
	if err != nil {
		return nil, err
	}

	err = store.ReplayWALs(ctx)
	if err != nil {
		return nil, err
	}
	db, _ := store.DB(ctx, "prometheus")
	schema, err := promSchema()
	if err != nil {
		return nil, err
	}
	table, err := db.Table(
		"metrics",
		frost.NewTableConfig(schema),
	)
	if err != nil {
		return nil, err
	}
	return &FrostDB{
		db:     db,
		store:  store,
		schema: schema,
		table:  table,
	}, nil
}

func (f *FrostDB) Query() error {
	return nil
}

func (f *FrostQuerier) LabelValues(name string, matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	engine := query.NewEngine(
		memory.NewGoAllocator(),
		f.db.TableProvider(),
	)

	sets := map[uint64]*series{}
	err := engine.ScanTable("metrics").
		Filter(promMatchersToFrostDBExprs(matchers)).
		Distinct(logicalplan.Col("labels."+name)).
		Execute(context.Background(), func(ar arrow.Record) error {
			defer ar.Release()
			parseRecordIntoSeriesSet(ar, sets)
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf(" failed to perform labels query: %v", err)
	}

	s := flattenSeriesSets(sets)
	names := []string{}
	for _, s := range s.sets {
		for _, l := range s.l {
			names = append(names, l.Value)
		}
	}

	return names, nil, nil
}

func promMatchersToFrostDBExprs(matchers []*labels.Matcher) logicalplan.Expr {
	exprs := []logicalplan.Expr{}
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
	return logicalplan.And(exprs...)
}

func (f *FrostQuerier) LabelNames(matchers ...*labels.Matcher) ([]string, storage.Warnings, error) {
	engine := query.NewEngine(
		memory.NewGoAllocator(),
		f.db.TableProvider(),
	)

	sets := map[string]struct{}{}
	err := engine.ScanTable("metrics").
		Project(logicalplan.DynCol("labels")).
		Filter(promMatchersToFrostDBExprs(matchers)).
		Execute(context.Background(), func(ar arrow.Record) error {
			defer ar.Release()
			for i := 0; i < int(ar.NumCols()); i++ {
				sets[ar.ColumnName(i)] = struct{}{}
			}
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf(" failed to perform labels query: %v", err)
	}

	names := []string{}
	for s := range sets {
		names = append(names, strings.TrimPrefix(s, "labels."))
	}

	return names, nil, nil
}

func (f *FrostQuerier) Close() error { return nil }

func (f *FrostQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	engine := query.NewEngine(
		memory.NewGoAllocator(),
		f.db.TableProvider(),
	)

	sets := map[uint64]*series{}
	err := engine.ScanTable("metrics").
		Filter(logicalplan.And(
			logicalplan.And(
				logicalplan.Col("timestamp").Gt(logicalplan.Literal(hints.Start)),
				logicalplan.Col("timestamp").Lt(logicalplan.Literal(hints.End)),
			),
			promMatchersToFrostDBExprs(matchers),
		)).
		Project(
			logicalplan.DynCol("labels"),
			logicalplan.Col("timestamp"),
			logicalplan.Col("value"),
		).
		Execute(context.Background(), func(ar arrow.Record) error {
			defer ar.Release()
			parseRecordIntoSeriesSet(ar, sets)
			return nil
		})
	if err != nil {
		fmt.Println("error: ", err)
	}
	return flattenSeriesSets(sets)
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

func promSchema() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "metrics_schema",
		Columns: []*schemapb.Column{{
			Name: "labels",
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: "timestamp",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_INT64,
			},
			Dynamic: false,
		}, {
			Name: "value",
			StorageLayout: &schemapb.StorageLayout{
				Type: schemapb.StorageLayout_TYPE_DOUBLE,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{{
			Name:       "labels",
			NullsFirst: true,
			Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
		},
			{
				Name:      "timestamp",
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			}},
	})
}

type arrowSeriesSet struct {
	index int
	sets  []*series
}

type arrowSeries struct {
	index int
	*series
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
	for ; a.index < len(a.series.ts); a.index++ {
		if a.series.ts[a.index] >= i {
			return true
		}
	}
	return false
}

func (a *arrowSeries) At() (int64, float64) {
	return a.ts[a.index], a.v[a.index]
}

func (a *arrowSeries) Err() error { return nil }

func parseRecordIntoSeriesSet(ar arrow.Record, sets map[uint64]*series) {
	seriesset := parseRecord(ar)
	for id, set := range seriesset {
		if s, ok := sets[id]; ok {
			s.ts, s.v = merge(s.ts, set.ts, s.v, set.v)
		} else {
			sets[id] = set
		}
	}
}

func flattenSeriesSets(sets map[uint64]*series) *arrowSeriesSet {
	// Flatten sets
	ss := []*series{}
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

func parseRecord(r arrow.Record) map[uint64]*series {

	seriesset := map[uint64]*series{}

	for i := 0; i < int(r.NumRows()); i++ {
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
				name := strings.TrimPrefix(r.ColumnName(j), "labels.")
				value := r.Column(j).(*array.Binary).Value(i)
				if string(value) != "" {
					lbls = append(lbls, labels.Label{
						Name:  name,
						Value: string(value),
					})
				}
			}
		}
		h := lbls.Hash()
		if es, ok := seriesset[h]; ok {
			es.ts = append(es.ts, ts)
			es.v = append(es.v, v)
		} else {
			seriesset[h] = &series{
				ts: []int64{ts},
				v:  []float64{v},
				l:  lbls,
			}
		}
	}

	return seriesset
}

// merge's a,b into an ordered list, maintains this same order for the floats
func merge(a, b []int64, af, bf []float64) ([]int64, []float64) {

	ai := 0
	bi := 0

	result := []int64{}
	floats := []float64{}
	for {
		av := int64(math.MaxInt64)
		bv := int64(math.MaxInt64)

		if ai < len(a) {
			av = a[ai]
		}

		if bi < len(b) {
			bv = b[bi]
		}

		if av == math.MaxInt64 && bv == math.MaxInt64 {
			return result, floats
		}

		var min int64
		var f float64
		switch {
		case av <= bv:
			min = av
			f = af[ai]
			ai++
		default:
			min = bv
			f = bf[bi]
			bi++
		}
		result = append(result, min)
		floats = append(floats, f)
	}
}
