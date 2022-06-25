package frostdb

import (
	"fmt"
	"testing"

	"github.com/apache/arrow/go/v8/arrow"
	"github.com/apache/arrow/go/v8/arrow/array"
	"github.com/apache/arrow/go/v8/arrow/memory"
)

func Test_Arrow_Record_SeriesSet(t *testing.T) {
	mem := memory.NewGoAllocator()

	/*
	  col[0][labels.__name__]: ["go_goroutines"]
	  col[1][labels.instance]: ["localhost:9090"]
	  col[2][labels.job]: ["prometheus"]
	  col[3][timestamp]: [1656116098515]
	  col[4][value]: [30]
	*/
	schema := arrow.NewSchema(
		[]arrow.Field{
			{Name: "labels.__name__", Type: arrow.BinaryTypes.String},
			{Name: "labels.instance", Type: arrow.BinaryTypes.String},
			{Name: "labels.job", Type: arrow.BinaryTypes.String},
			{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil, // no metadata
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.StringBuilder).AppendValues(
		[]string{"go_goroutines"},
		nil,
	)
	b.Field(1).(*array.StringBuilder).AppendValues(
		[]string{"localhost:9090"},
		nil,
	)
	b.Field(2).(*array.StringBuilder).AppendValues(
		[]string{"epimetheus"},
		nil,
	)
	b.Field(3).(*array.Int64Builder).AppendValues(
		[]int64{1656116098515},
		nil,
	)
	b.Field(4).(*array.Float64Builder).AppendValues(
		[]float64{30},
		nil,
	)

	rec1 := b.NewRecord()
	defer rec1.Release()

	fmt.Println(rec1)
}
