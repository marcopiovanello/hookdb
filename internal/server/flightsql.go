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
	"fmt"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/marcopiovanello/hookdb/internal/catalog"
)

type Server struct {
	flightsql.BaseServer

	db  *duckdb.Conn
	cat *catalog.Catalog
	mem memory.Allocator
}

func New(db *duckdb.Conn, cat *catalog.Catalog) *Server {
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

	ar, err := duckdb.NewArrowFromConn(s.db)
	if err != nil {
		return nil, nil, err
	}

	reader, err := ar.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	schema := reader.Schema()
	ch := make(chan flight.StreamChunk)

	go func() {
		for reader.Next() {
			record := reader.RecordBatch()
			ch <- flight.StreamChunk{Data: record}
		}
		if reader.Err() != nil {
			ch <- flight.StreamChunk{Err: err}
		}
	}()

	return schema, ch, nil
}

func (s *Server) schemaForQuery(ctx context.Context, query string) (*arrow.Schema, error) {
	probe := fmt.Sprintf("SELECT * FROM (%s) AS _tf_probe LIMIT 0", query)

	ar, err := duckdb.NewArrowFromConn(s.db)
	if err != nil {
		return nil, err
	}

	reader, err := ar.QueryContext(ctx, probe)
	if err != nil {
		reader, err = ar.QueryContext(ctx, query)
		if err != nil {
			return nil, err
		}
	}

	defer reader.Release()
	return reader.Schema(), reader.Err()
}

func (s *Server) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	ar, err := duckdb.NewArrowFromConn(s.db)
	if err != nil {
		return nil, nil, err
	}

	reader, err := ar.QueryContext(ctx, "SELECT DISTINCT catalog_name FROM information_schema.schemata")
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	schema := reader.Schema()
	record := reader.RecordBatch()

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: record}
	close(ch)

	return schema, ch, reader.Err()
}

// TODO: DoGetDBSchemas / GetFlightInfoSchemas
// TODO: DoGetTables / GetFlightInfoTables  (s.cat.Tables())
// TODO: DoGetTableTypes / GetFlightInfoTableTypes
// TODO: DoGetSqlInfo / GetFlightInfoSqlInfo
