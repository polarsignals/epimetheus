package main

import (
	"context"
	"sort"

	"github.com/apache/arrow/go/v10/arrow/memory"
	"github.com/polarsignals/frostdb"
	"github.com/thanos-io/objstore/providers/filesystem"

	epifrost "github.com/prometheus/prometheus/frostdb"
	"github.com/prometheus/prometheus/model/labels"
	"github.com/prometheus/prometheus/tsdb/chunkenc"
	"github.com/prometheus/prometheus/tsdb/chunks"
	tsdb_errors "github.com/prometheus/prometheus/tsdb/errors"
	"github.com/prometheus/prometheus/tsdb/index"
)

func convertBlockFrostDB(path, blockID string) error {
	//f, err := os.Create("frostdb-convert-cpu.prof")
	//if err != nil {
	//	return err
	//}
	//defer f.Close() // error handling omitted for example
	//if err := pprof.StartCPUProfile(f); err != nil {
	//	return err
	//}
	//defer pprof.StopCPUProfile()

	tsdb, block, err := openBlock(path, blockID)
	if err != nil {
		return err
	}
	defer func() {
		tsdb_errors.NewMulti(err, tsdb.Close()).Err()
	}()

	ir, err := block.Index()
	if err != nil {
		return err
	}
	defer ir.Close()

	postingsr, err := ir.Postings(index.AllPostingsKey())
	if err != nil {
		return err
	}
	chunkr, err := block.Chunks()
	if err != nil {
		return err
	}
	defer func() {
		err = tsdb_errors.NewMulti(err, chunkr.Close()).Err()
	}()

	// FrostDB

	bucket, err := filesystem.NewBucket("data-promtool")
	if err != nil {
		return err
	}
	store, err := frostdb.New(
		frostdb.WithBucketStorage(bucket),
	)
	if err != nil {
		return err
	}

	db, err := store.DB(context.Background(), "prometheus")
	if err != nil {
		return err
	}
	tableSchema, err := epifrost.SchemaMetrics()
	if err != nil {
		return err
	}
	table, err := db.Table(
		epifrost.TableMetrics,
		frostdb.NewTableConfig(tableSchema),
	)
	if err != nil {
		return err
	}

	chks := []chunks.Meta{}
	builder := labels.ScratchBuilder{}

	labelNamesMap := map[string]struct{}{}
	for postingsr.Next() {
		if err := ir.Series(postingsr.At(), &builder, &chks); err != nil {
			return err
		}
		for name := range builder.Labels().Map() {
			labelNamesMap[name] = struct{}{}
		}
	}
	if postingsr.Err() != nil {
		return postingsr.Err()
	}

	labelNames := make([]string, 0, len(labelNamesMap))
	for name := range labelNamesMap {
		labelNames = append(labelNames, name)
	}
	sort.Strings(labelNames)

	mem := memory.NewGoAllocator()

	rb := epifrost.NewRecordBuilder(mem, labelNames)

	// Reset the postings reader by creating a new one. Seek doesn't work.
	postingsr, err = ir.Postings(index.AllPostingsKey())
	if err != nil {
		return err
	}
	var it chunkenc.Iterator
	for postingsr.Next() {
		if err := ir.Series(postingsr.At(), &builder, &chks); err != nil {
			return err
		}

		lset := builder.Labels()

		for _, chk := range chks {
			chk, err := chunkr.Chunk(chk)
			if err != nil {
				return err
			}

			it = chk.Iterator(it)
			for it.Next() == chunkenc.ValFloat {
				t, v := it.At()
				rb.Append(lset, t, v)
			}
		}
	}

	r := rb.NewRecord()
	defer r.Release()

	_, err = table.InsertRecord(context.Background(), r)
	if err != nil {
		return err
	}

	return store.Close()
}
