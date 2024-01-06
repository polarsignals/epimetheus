package frostdb

import (
	"github.com/apache/arrow/go/v10/arrow"
	"github.com/apache/arrow/go/v10/arrow/array"
	"github.com/apache/arrow/go/v10/arrow/memory"

	"github.com/prometheus/prometheus/model/labels"
)

type RecordBuilder struct {
	mem              memory.Allocator
	labelNames       []string
	fields           []arrow.Field
	builders         []*array.BinaryBuilder
	builderTimestamp *array.Int64Builder
	builderValue     *array.Float64Builder
}

func NewRecordBuilder(mem memory.Allocator, labelNames []string) *RecordBuilder {
	rb := &RecordBuilder{mem: mem, labelNames: labelNames}
	rb.fields = make([]arrow.Field, 0, len(labelNames)+2)
	rb.builders = make([]*array.BinaryBuilder, 0, len(labelNames))

	for _, k := range labelNames {
		rb.fields = append(rb.fields, arrow.Field{Name: "labels." + k, Type: arrow.BinaryTypes.String})
		rb.builders = append(rb.builders, array.NewBinaryBuilder(rb.mem, arrow.BinaryTypes.String))
	}

	rb.fields = append(rb.fields, arrow.Field{Name: ColumnTimestamp, Type: arrow.PrimitiveTypes.Int64})
	rb.builderTimestamp = array.NewInt64Builder(rb.mem)

	rb.fields = append(rb.fields, arrow.Field{Name: ColumnValue, Type: arrow.PrimitiveTypes.Float64})
	rb.builderValue = array.NewFloat64Builder(rb.mem)
	return rb
}

func (rb *RecordBuilder) Append(labels labels.Labels, timestamp int64, value float64) {
	for i, ln := range rb.labelNames {
		lv := labels.Get(ln)
		if lv == "" {
			rb.builders[i].AppendNull()
		} else {
			rb.builders[i].AppendString(lv)
		}
	}
	rb.builderTimestamp.Append(timestamp)
	rb.builderValue.Append(value)
}

func (rb *RecordBuilder) NewRecord() arrow.Record {
	rows := rb.builderValue.Len()

	arrays := make([]arrow.Array, 0, len(rb.labelNames)+2)
	for _, b := range rb.builders {
		arrays = append(arrays, b.NewArray())
		b.Release()
	}
	arrays = append(arrays, rb.builderTimestamp.NewArray())
	arrays = append(arrays, rb.builderValue.NewArray())
	rb.builderTimestamp.Release()
	rb.builderValue.Release()

	schema := arrow.NewSchema(rb.fields, nil)
	return array.NewRecord(schema, arrays, int64(rows))
}
