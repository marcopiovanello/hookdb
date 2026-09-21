// Package sqlarrow provides utilties for the interaction between Apache Arrow
// and database/sql compabible drivers.
package sqlarrow

import (
	"database/sql"
	"fmt"
	"log/slog"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// Derives an Arrow schema from *sql.Rows values
func BuildSchema(rows *sql.Rows) (*arrow.Schema, error) {
	cts, err := rows.ColumnTypes()
	if err != nil {
		return nil, fmt.Errorf("column types: %w", err)
	}

	fields := make([]arrow.Field, len(cts))

	for i, ct := range cts {
		nullable, _ := ct.Nullable()
		fields[i] = arrow.Field{
			Name:     ct.Name(),
			Type:     arrowTypeFor(ct),
			Nullable: nullable,
		}
	}

	return arrow.NewSchema(fields, nil), nil
}

func arrowTypeFor(ct *sql.ColumnType) arrow.DataType {
	switch ct.DatabaseTypeName() {
	case "BOOLEAN":
		return arrow.FixedWidthTypes.Boolean
	case "TINYINT":
		return arrow.PrimitiveTypes.Int8
	case "SMALLINT":
		return arrow.PrimitiveTypes.Int16
	case "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT", "HUGEINT":
		return arrow.PrimitiveTypes.Int64
	case "UTINYINT":
		return arrow.PrimitiveTypes.Uint8
	case "USMALLINT":
		return arrow.PrimitiveTypes.Uint16
	case "UINTEGER":
		return arrow.PrimitiveTypes.Uint32
	case "UBIGINT":
		return arrow.PrimitiveTypes.Uint64
	case "FLOAT":
		return arrow.PrimitiveTypes.Float32
	case "DOUBLE", "DECIMAL":
		return arrow.PrimitiveTypes.Float64
	case "TIMESTAMP", "TIMESTAMP WITH TIME ZONE", "TIMESTAMP_NS":
		return arrow.FixedWidthTypes.Timestamp_ns
	case "DATE":
		return arrow.FixedWidthTypes.Date32
	case "VARCHAR", "BLOB":
		return arrow.BinaryTypes.String
	default:
		return arrow.BinaryTypes.String
	}
}

func RowsToRecords(
	mem memory.Allocator,
	schema *arrow.Schema,
	rows *sql.Rows,
	maxBatch int,
	emit func(arrow.RecordBatch) error,
) error {
	if maxBatch <= 0 {
		maxBatch = 4096
	}

	var (
		n       = len(schema.Fields())
		rawVals = make([]any, n)
		scanDst = make([]any, n)
	)

	for i := range rawVals {
		scanDst[i] = &rawVals[i]
	}

	builder := array.NewRecordBuilder(mem, schema)
	defer builder.Release()

	count := 0

	flush := func() error {
		if count == 0 {
			return nil
		}

		rec := builder.NewRecordBatch()
		count = 0

		return emit(rec)
	}

	for rows.Next() {
		if err := rows.Scan(scanDst...); err != nil {
			return fmt.Errorf("scan row: %w", err)
		}

		for i, f := range schema.Fields() {
			appendValue(builder.Field(i), f.Type, rawVals[i])
		}

		count++

		if count >= maxBatch {
			if err := flush(); err != nil {
				return err
			}
		}
	}

	if err := rows.Err(); err != nil {
		return fmt.Errorf("rows iteration: %w", err)
	}

	return flush()
}

func appendValue(b array.Builder, t arrow.DataType, v any) {
	if v == nil {
		b.AppendNull()
		return
	}
	switch bb := b.(type) {
	case *array.BooleanBuilder:
		if x, ok := v.(bool); ok {
			bb.Append(x)
			return
		}
	case *array.Int8Builder:
		if x, ok := toTyped[int8](v); ok {
			bb.Append(x)
			return
		}
	case *array.Int16Builder:
		if x, ok := toTyped[int16](v); ok {
			bb.Append(x)
			return
		}
	case *array.Int32Builder:
		if x, ok := toTyped[int32](v); ok {
			bb.Append(x)
			return
		}
	case *array.Int64Builder:
		if x, ok := toTyped[int64](v); ok {
			bb.Append(x)
			return
		}
	case *array.Uint8Builder:
		if x, ok := toTyped[uint8](v); ok {
			bb.Append(x)
			return
		}
	case *array.Uint16Builder:
		if x, ok := toTyped[uint16](v); ok {
			bb.Append(x)
			return
		}
	case *array.Uint32Builder:
		if x, ok := toTyped[uint32](v); ok {
			bb.Append(x)
			return
		}
	case *array.Uint64Builder:
		if x, ok := toTyped[uint64](v); ok {
			bb.Append(x)
			return
		}
	case *array.Float32Builder:
		if x, ok := toTyped[float32](v); ok {
			bb.Append(x)
			return
		}
	case *array.Float64Builder:
		if x, ok := toTyped[float64](v); ok {
			bb.Append(x)
			return
		}
	case *array.TimestampBuilder:
		if x, ok := v.(time.Time); ok {
			bb.Append(arrow.Timestamp(x.UnixNano()))
			return
		}
	case *array.Date32Builder:
		if x, ok := v.(time.Time); ok {
			bb.Append(arrow.Date32FromTime(x))
			return
		}
	case *array.StringBuilder:
		bb.Append(fmt.Sprint(v))
		return
	}

	slog.Error("failed to map value to arrow type", slog.String("id", t.ID().String()))
	b.AppendNull()
}
