// Package server implements the base surface of the Arrow Flight SQL engine.
//
// TODO: GetFlightInfoStatement (perform SQL query) and DoGetStatement (retrieve data) are implemented.
// The other method are left as stubs with the default implementation of the go-arrow library.
// A future implementation of the left stubs will provide autocomplete for Grafana.
//
// TODO: copy information_schema / pragma_* impl. from duckdb and create the translation layer of Arrow
package server

import (
	"context"
	"database/sql"
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"

	"github.com/marcopiovanello/hookdb/internal/catalog"
	"github.com/marcopiovanello/hookdb/internal/sqlarrow"
)

const batchSize = 8192

type Server struct {
	flightsql.BaseServer

	db  *sql.DB
	cat *catalog.Catalog
	mem memory.Allocator
}

func New(db *sql.DB, cat *catalog.Catalog) *Server {
	return &Server{
		db:  db,
		cat: cat,
		mem: memory.NewGoAllocator(),
	}
}

func (s *Server) GetFlightInfoStatement(
	ctx context.Context,
	cmd flightsql.StatementQuery,
	desc *flight.FlightDescriptor,
) (
	*flight.FlightInfo,
	error,
) {
	query := cmd.GetQuery()

	schema, err := s.schemaForQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("cannot infer query schema: %w", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(query))
	if err != nil {
		return nil, fmt.Errorf("failed creating query ticket: %w", err)
	}

	info := &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.mem),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: ticketBytes}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}
	return info, nil
}

func (s *Server) DoGetStatement(
	ctx context.Context,
	ticket flightsql.StatementQueryTicket,
) (
	*arrow.Schema,
	<-chan flight.StreamChunk,
	error,
) {
	query := string(ticket.GetStatementHandle())

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("failed while executing query: %w", err)
	}

	schema, err := sqlarrow.BuildSchema(rows)
	if err != nil {
		rows.Close()
		return nil, nil, err
	}

	ch := make(chan flight.StreamChunk)

	go func() {
		defer rows.Close()
		defer close(ch)

		err := sqlarrow.RowsToRecords(s.mem, schema, rows, batchSize, func(rec arrow.Record) error {
			ch <- flight.StreamChunk{Data: rec}
			return nil
		})
		if err != nil {
			ch <- flight.StreamChunk{Err: err}
		}
	}()

	return schema, ch, nil
}

func (s *Server) schemaForQuery(ctx context.Context, query string) (*arrow.Schema, error) {
	probe := fmt.Sprintf("SELECT * FROM (%s) AS _tf_probe LIMIT 0", query)

	rows, err := s.db.QueryContext(ctx, probe)
	if err != nil {
		rows, err = s.db.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
	}

	defer rows.Close()
	return sqlarrow.BuildSchema(rows)
}

func (s *Server) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	schema := arrow.NewSchema([]arrow.Field{
		{Name: "catalog_name", Type: arrow.BinaryTypes.String},
	}, nil)

	rows, err := s.db.QueryContext(ctx, "SELECT DISTINCT catalog_name FROM information_schema.schemata")
	if err != nil {
		return nil, nil, err
	}
	defer rows.Close()

	b := array.NewRecordBuilder(s.mem, schema)
	defer b.Release()

	strBuilder := b.Field(0).(*array.StringBuilder)

	for rows.Next() {
		var name string
		if err := rows.Scan(&name); err != nil {
			return nil, nil, err
		}
		strBuilder.Append(name)
	}

	if err := rows.Err(); err != nil {
		return nil, nil, err
	}

	rec := b.NewRecordBatch()

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: rec}
	close(ch)

	return schema, ch, nil
}

// TODO: DoGetDBSchemas / GetFlightInfoSchemas
// TODO: DoGetTables / GetFlightInfoTables  (s.cat.Tables())
// TODO: DoGetTableTypes / GetFlightInfoTableTypes
// TODO: DoGetSqlInfo / GetFlightInfoSqlInfo
