package frostdb

import (
	"context"
	"fmt"
	"math"
	"sort"
	"strings"

	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	frost "github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	schemapb "github.com/polarsignals/frostdb/gen/proto/go/frostdb/schema/v1alpha1"
	"github.com/polarsignals/frostdb/query"
	"github.com/polarsignals/frostdb/query/logicalplan"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/thanos-io/objstore/providers/filesystem"
	"golang.org/x/exp/maps"

	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/histogram"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/model/metadata"
	"github.com/prometheus/prometheus/storage"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
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
	mem      memory.Allocator
	buffer   *MetricBuffer
}

func (f *FrostAppender) AppendHistogram(ref storage.SeriesRef, l labels.Labels, t int64, h *histogram.Histogram, fh *histogram.FloatHistogram) (storage.SeriesRef, error) {
	panic("histogram not supported")
}

func (f *FrostAppender) UpdateMetadata(ref storage.SeriesRef, l labels.Labels, m metadata.Metadata) (storage.SeriesRef, error) {
	panic("metadata not supported")
}

type FrostQuerier struct {
	*FrostDB
}

// Open a new frostDB
func Open(dir string, reg prometheus.Registerer, logger log.Logger) (*FrostDB, error) {
	bucket, err := filesystem.NewBucket(dir)
	ctx := context.Background()
	store, err := frost.New(
		frost.WithLogger(logger),
		frost.WithRegistry(reg),
		frost.WithWAL(),
		frost.WithStoragePath(dir),
		frost.WithBucketStorage(bucket),
	)
	if err != nil {
		return nil, err
	}

	db, _ := store.DB(ctx, "prometheus")
	schema, err := SchemaMetrics()
	if err != nil {
		return nil, err
	}
	table, err := db.Table(
		TableMetrics,
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
	err := engine.ScanTable(TableMetrics).
		Filter(promMatchersToFrostDBExprs(matchers)).
		Distinct(logicalplan.Col("labels."+name)).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
			defer ar.Release()
			parseRecordIntoSeriesSet(ar, sets)
			return nil
		})
	if err != nil {
		return nil, nil, fmt.Errorf(" failed to perform labels query: %v", err)
	}

	s := flattenSeriesSets(sets)
	names := make([]string, 0, len(s.sets))
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
	err := engine.ScanTable(TableMetrics).
		Project(logicalplan.DynCol(ColumnLabels)).
		Filter(promMatchersToFrostDBExprs(matchers)).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
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

func (f *FrostQuerier) Close() error {
	return nil
}

func (f *FrostQuerier) Select(sortSeries bool, hints *storage.SelectHints, matchers ...*labels.Matcher) storage.SeriesSet {
	engine := query.NewEngine(
		memory.NewGoAllocator(),
		f.db.TableProvider(),
	)

	sets := map[uint64]*series{}
	err := engine.ScanTable(TableMetrics).
		Filter(logicalplan.And(
			logicalplan.And(
				logicalplan.Col(ColumnTimestamp).Gt(logicalplan.Literal(hints.Start)),
				logicalplan.Col(ColumnTimestamp).Lt(logicalplan.Literal(hints.End)),
			),
			promMatchersToFrostDBExprs(matchers),
		)).
		Project(
			logicalplan.DynCol(ColumnLabels),
			logicalplan.Col(ColumnTimestamp),
			logicalplan.Col(ColumnValue),
		).
		Execute(context.Background(), func(ctx context.Context, ar arrow.Record) error {
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
	return f.store.Close()
}

func (f *FrostDB) Appender(ctx context.Context) storage.Appender {
	return &FrostAppender{
		ctx:      ctx,
		schema:   f.schema,
		tableRef: f.table,
		mem:      memory.NewGoAllocator(),
	}
}

type MetricBuffer struct {
	columns map[string]struct{}
	samples []*MetricSample
}

type MetricSample struct {
	l labels.Labels
	t int64
	v float64
}

// Append writes immediately to the frostdb. Rollback and Commit are nop
func (f *FrostAppender) Append(ref storage.SeriesRef, l labels.Labels, t int64, v float64) (storage.SeriesRef, error) {
	if f.buffer == nil {
		f.buffer = &MetricBuffer{
			columns: make(map[string]struct{}),
			samples: make([]*MetricSample, 0, 64),
		}
	}

	for _, l := range l {
		f.buffer.columns[l.Name] = struct{}{}
	}

	f.buffer.samples = append(f.buffer.samples, &MetricSample{l: l, t: t, v: v})

	return 0, nil
}

func (f *FrostAppender) Commit() error {
	keys := maps.Keys(f.buffer.columns)
	sort.Strings(keys)

	rb := NewRecordBuilder(f.mem, keys)

	for _, s := range f.buffer.samples {
		rb.Append(s.l, s.t, s.v)
	}

	r := rb.NewRecord()
	defer r.Release()

	_, err := f.tableRef.InsertRecord(f.ctx, r)
	if err != nil {
		return err
	}

	// reset buffer
	f.buffer.columns = make(map[string]struct{})
	f.buffer.samples = f.buffer.samples[:0]
	return nil
}

func (f *FrostAppender) Rollback() error { return nil }

func (f *FrostAppender) AppendExemplar(ref storage.SeriesRef, l labels.Labels, e exemplar.Exemplar) (storage.SeriesRef, error) {
	return 0, nil
}

func (f *FrostDB) ChunkQuerier(ctx context.Context, mint, maxt int64) (storage.ChunkQuerier, error) {
	return nil, nil
}

const (
	TableMetrics    = "metrics"
	ColumnLabels    = "labels"
	ColumnTimestamp = "timestamp"
	ColumnValue     = "value"
)

func SchemaMetrics() (*dynparquet.Schema, error) {
	return dynparquet.SchemaFromDefinition(&schemapb.Schema{
		Name: "metrics_schema",
		Columns: []*schemapb.Column{{
			Name: ColumnLabels,
			StorageLayout: &schemapb.StorageLayout{
				Type:     schemapb.StorageLayout_TYPE_STRING,
				Encoding: schemapb.StorageLayout_ENCODING_RLE_DICTIONARY,
				Nullable: true,
			},
			Dynamic: true,
		}, {
			Name: ColumnTimestamp,
			StorageLayout: &schemapb.StorageLayout{
				Type:        schemapb.StorageLayout_TYPE_INT64,
				Compression: schemapb.StorageLayout_COMPRESSION_ZSTD,
			},
			Dynamic: false,
		}, {
			Name: ColumnValue,
			StorageLayout: &schemapb.StorageLayout{
				Type:        schemapb.StorageLayout_TYPE_DOUBLE,
				Compression: schemapb.StorageLayout_COMPRESSION_ZSTD,
			},
			Dynamic: false,
		}},
		SortingColumns: []*schemapb.SortingColumn{
			{
				Name:       ColumnLabels,
				NullsFirst: true,
				Direction:  schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
			{
				Name:      ColumnTimestamp,
				Direction: schemapb.SortingColumn_DIRECTION_ASCENDING,
			},
		},
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

func (a *arrowSeries) AtHistogram() (int64, *histogram.Histogram) {
	panic("histogram not supported")
}

func (a *arrowSeries) AtFloatHistogram() (int64, *histogram.FloatHistogram) {
	panic("histogram not supported")
}

func (a *arrowSeries) AtT() int64 {
	return a.series.ts[a.index]
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

func (a *arrowSeries) Iterator(iterator chunkenc.Iterator) chunkenc.Iterator {
	// TODO: Use iterator here?
	return a
}

func (a *arrowSeries) Next() chunkenc.ValueType {
	a.index++
	if a.index < len(a.ts) {
		return chunkenc.ValFloat
	}
	return chunkenc.ValNone
}

func (a *arrowSeries) Seek(i int64) chunkenc.ValueType {
	for ; a.index < len(a.series.ts); a.index++ {
		if a.series.ts[a.index] >= i {
			return chunkenc.ValFloat
		}
	}
	return chunkenc.ValNone
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
			columnName := r.ColumnName(j)
			switch {
			case columnName == ColumnTimestamp:
				ts = r.Column(j).(*array.Int64).Value(i)
			case columnName == ColumnValue:
				v = r.Column(j).(*array.Float64).Value(i)
			default:
				name := strings.TrimPrefix(columnName, "labels.")
				nameColumn, err := DictionaryFromRecord(r, columnName)
				if err != nil {
					continue
				}
				if nameColumn.IsNull(i) {
					continue
				}

				value := StringValueFromDictionary(nameColumn, i)
				if string(value) != "" {
					lbls = append(lbls, labels.Label{
						Name:  name,
						Value: value,
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

func DictionaryFromRecord(ar arrow.Record, name string) (*array.Dictionary, error) {
	indices := ar.Schema().FieldIndices(name)
	if len(indices) != 1 {
		return nil, fmt.Errorf("expected 1 column named %q, got %d", name, len(indices))
	}

	col, ok := ar.Column(indices[0]).(*array.Dictionary)
	if !ok {
		return nil, fmt.Errorf("expected column %q to be a dictionary column, got %T", name, ar.Column(indices[0]))
	}

	return col, nil
}

func StringValueFromDictionary(arr *array.Dictionary, i int) string {
	switch dict := arr.Dictionary().(type) {
	case *array.Binary:
		return string(dict.Value(arr.GetValueIndex(i)))
	case *array.String:
		return dict.Value(arr.GetValueIndex(i))
	default:
		panic(fmt.Sprintf("unsupported dictionary type: %T", dict))
	}
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
