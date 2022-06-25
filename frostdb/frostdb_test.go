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
			{Name: "labels.__name__", Type: arrow.BinaryTypes.Binary},
			{Name: "labels.instance", Type: arrow.BinaryTypes.Binary},
			{Name: "labels.job", Type: arrow.BinaryTypes.Binary},
			{Name: "timestamp", Type: arrow.PrimitiveTypes.Int64},
			{Name: "value", Type: arrow.PrimitiveTypes.Float64},
		},
		nil, // no metadata
	)

	b := array.NewRecordBuilder(mem, schema)
	defer b.Release()

	b.Field(0).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("go_goroutines")},
		nil,
	)
	b.Field(1).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("localhost:9090")},
		nil,
	)
	b.Field(2).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("epimetheus")},
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

	b.Field(0).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("go_goroutines")},
		nil,
	)
	b.Field(1).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("localhost:9090")},
		nil,
	)
	b.Field(2).(*array.BinaryBuilder).AppendValues(
		[][]byte{[]byte("epimetheus")},
		nil,
	)
	b.Field(3).(*array.Int64Builder).AppendValues(
		[]int64{165611609999},
		nil,
	)
	b.Field(4).(*array.Float64Builder).AppendValues(
		[]float64{30},
		nil,
	)
	rec2 := b.NewRecord()
	defer rec2.Release()

	fmt.Println("rec1: ", rec1)
	fmt.Println("rec2: ", rec2)

	sset := seriesSetFromRecords([]arrow.Record{rec1, rec2})
	for sset.Next() {
		set := sset.At()
		fmt.Println("Labels: ", set.Labels())
		it := set.Iterator()
		for it.Next() {
			ts, v := it.At()
			fmt.Println("At: ", ts, v)
		}
	}
}

func Test_Merge(t *testing.T) {
	fmt.Println(merge([]int64{0, 1, 4}, []int64{2, 3, 7}, []float64{1, 1, 1}, []float64{5, 5, 5}))
}
