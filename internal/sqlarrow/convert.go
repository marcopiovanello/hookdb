package sqlarrow

import (
	"database/sql"
	"fmt"
	"time"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/memory"
)

// BuildSchema deriva uno schema Arrow dalle colonne di una *sql.Rows già
// aperta (va chiamata prima di consumare le righe).
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
	case "TINYINT", "SMALLINT", "INTEGER":
		return arrow.PrimitiveTypes.Int32
	case "BIGINT", "HUGEINT":
		return arrow.PrimitiveTypes.Int64
	case "UTINYINT", "USMALLINT", "UINTEGER":
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

func RowsToRecords(mem memory.Allocator, schema *arrow.Schema, rows *sql.Rows, maxBatch int, emit func(arrow.RecordBatch) error) error {
	if maxBatch <= 0 {
		maxBatch = 4096
	}

	n := len(schema.Fields())
	rawVals := make([]interface{}, n)
	scanDst := make([]interface{}, n)
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
		rec := builder.NewRecord()
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
	case *array.Int32Builder:
		if x, ok := toInt64(v); ok {
			bb.Append(int32(x))
			return
		}
	case *array.Int64Builder:
		if x, ok := toInt64(v); ok {
			bb.Append(x)
			return
		}
	case *array.Uint32Builder:
		if x, ok := toInt64(v); ok {
			bb.Append(uint32(x))
			return
		}
	case *array.Uint64Builder:
		if x, ok := toInt64(v); ok {
			bb.Append(uint64(x))
			return
		}
	case *array.Float32Builder:
		if x, ok := toFloat64(v); ok {
			bb.Append(float32(x))
			return
		}
	case *array.Float64Builder:
		if x, ok := toFloat64(v); ok {
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
	// Fallback estremo: se il tipo del valore non combacia con quanto
	// atteso (driver "esotico" o combinazione non gestita), evitiamo un
	// panic e appendiamo NULL. In produzione vale la pena loggare qui.
	b.AppendNull()
}

func toInt64(v any) (int64, bool) {
	switch x := v.(type) {
	case int64:
		return x, true
	case int32:
		return int64(x), true
	case int:
		return int64(x), true
	case uint64:
		return int64(x), true
	}
	return 0, false
}

func toFloat64(v any) (float64, bool) {
	switch x := v.(type) {
	case float64:
		return x, true
	case float32:
		return float64(x), true
	}
	return 0, false
}
