package frostdb

import (
	"context"

	"github.com/go-kit/log"
	"github.com/polarsignals/frostdb"
	frost "github.com/polarsignals/frostdb"
	"github.com/polarsignals/frostdb/dynparquet"
	"github.com/prometheus/client_golang/prometheus"
	"github.com/prometheus/prometheus/model/exemplar"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/storage"
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

// Open a new frostDB
func Open(reg prometheus.Registerer, logger log.Logger) (*FrostDB, error) {
	store := frost.New(
		reg,
		8192,
		10*1024*1024,
	)
	db, _ := store.DB("prometheus")
	table, err := db.Table(
		"metrics",
		frost.NewTableConfig(promSchema()),
		logger,
	)
	if err != nil {
		return nil, err
	}
	return &FrostDB{
		store: store,
		table: table,
	}, nil
}

func (f *FrostDB) Query() error {
	return nil
}

// Querier implements the storage.Queryable interface
func (f *FrostDB) Querier(ctx context.Context, mint, maxt int64) (storage.Querier, error) {
	return nil, nil
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
	row = append(row, parquet.ValueOf(t).Level(0, 0, 2))
	row = append(row, parquet.ValueOf(v).Level(0, 0, 3))

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
