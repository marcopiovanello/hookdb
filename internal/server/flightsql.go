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
	"log/slog"
	"regexp"
	"strings"

	"github.com/apache/arrow-go/v18/arrow"
	"github.com/apache/arrow-go/v18/arrow/array"
	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql/schema_ref"
	"github.com/apache/arrow-go/v18/arrow/memory"
	"github.com/duckdb/duckdb-go/v2"

	"github.com/marcopiovanello/hookdb/internal/catalog"
	"github.com/marcopiovanello/hookdb/internal/pool"
	"github.com/marcopiovanello/hookdb/pkg/sqlutil"
)

type Server struct {
	flightsql.BaseServer

	cat  *catalog.Catalog
	mem  memory.Allocator
	pool pool.Pool

	logger *slog.Logger
}

func New(pool pool.Pool, cat *catalog.Catalog, logger *slog.Logger) *Server {
	return &Server{
		cat:    cat,
		mem:    memory.NewGoAllocator(),
		pool:   pool,
		logger: logger,
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
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := string(ticket.GetStatementHandle())
	s.logger.DebugContext(ctx, "executing statement", "query", query)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}

	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		s.pool.Release(conn)
		return nil, nil, err
	}

	reader, err := ar.QueryContext(ctx, query)
	if err != nil {
		s.pool.Release(conn)
		return nil, nil, err
	}

	schema := reader.Schema()
	ch := make(chan flight.StreamChunk)

	go func() {
		defer reader.Release()
		defer s.pool.Release(conn)
		defer close(ch)

		for reader.Next() {
			record := reader.RecordBatch()
			record.Retain()

			select {
			case ch <- flight.StreamChunk{Data: record}:
			case <-ctx.Done():
				record.Release()
				return
			}
		}

		if reader.Err() != nil {
			select {
			case ch <- flight.StreamChunk{Err: reader.Err()}:
			case <-ctx.Done():
			}
		}
	}()

	return schema, ch, nil
}

func (s *Server) schemaForQuery(ctx context.Context, query string) (*arrow.Schema, error) {
	probe := fmt.Sprintf("SELECT * FROM (%s) AS _tf_probe LIMIT 0", query)

	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, err
	}
	defer s.pool.Release(conn)

	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return nil, err
	}

	reader, err := ar.QueryContext(ctx, probe)
	if err != nil {
		return nil, fmt.Errorf("cannot infer schema (query may not be a SELECT statement): %w", err)
	}
	defer reader.Release()

	return reader.Schema(), reader.Err()
}

func (s *Server) DoGetCatalogs(ctx context.Context) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	conn, err := s.pool.Acquire(ctx)
	if err != nil {
		return nil, nil, err
	}
	defer s.pool.Release(conn)

	ar, err := duckdb.NewArrowFromConn(conn)
	if err != nil {
		return nil, nil, err
	}

	reader, err := ar.QueryContext(ctx, "SELECT DISTINCT catalog_name FROM information_schema.schemata")
	if err != nil {
		return nil, nil, err
	}
	defer reader.Release()

	reader.Next()
	schema := reader.Schema()
	record := reader.RecordBatch()

	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: record}
	close(ch)

	return schema, ch, reader.Err()
}

// GetFlightInfoTables handles a CommandGetTables request. The
// FlightDescriptor's Cmd is already the serialized command, so we can reuse
// it verbatim as the ticket: DoGet will unmarshal it back into the same
// CommandGetTables and route it straight to DoGetTables.
func (s *Server) GetFlightInfoTables(
	ctx context.Context,
	cmd flightsql.GetTables,
	desc *flight.FlightDescriptor,
) (*flight.FlightInfo, error) {
	schema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		schema = schema_ref.TablesWithIncludedSchema
	}

	return &flight.FlightInfo{
		Schema:           flight.SerializeSchema(schema, s.mem),
		FlightDescriptor: desc,
		Endpoint: []*flight.FlightEndpoint{
			{Ticket: &flight.Ticket{Ticket: desc.Cmd}},
		},
		TotalRecords: -1,
		TotalBytes:   -1,
	}, nil
}

// DoGetTables lists the available tables found on the catalog.
// Any request made for other schemas/namespaces outside the catalog will return
// nothing.
func (s *Server) DoGetTables(
	ctx context.Context,
	cmd flightsql.GetTables,
) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	outSchema := schema_ref.Tables
	if cmd.GetIncludeSchema() {
		outSchema = schema_ref.TablesWithIncludedSchema
	}

	emptyResult := func() (*arrow.Schema, <-chan flight.StreamChunk, error) {
		ch := make(chan flight.StreamChunk)
		close(ch)
		return outSchema, ch, nil
	}

	if cmd.GetCatalog() != nil || cmd.GetDBSchemaFilterPattern() != nil {
		return emptyResult()
	}

	if types := cmd.GetTableTypes(); len(types) > 0 {
		wantsTable := false
		for _, t := range types {
			if strings.EqualFold(t, "TABLE") {
				wantsTable = true
				break
			}
		}
		if !wantsTable {
			return emptyResult()
		}
	}

	var nameFilter *regexp.Regexp
	if p := cmd.GetTableNameFilterPattern(); p != nil {
		var err error
		nameFilter, err = sqlutil.LikeToRegexp(*p)
		if err != nil {
			return nil, nil, fmt.Errorf("invalid table name filter pattern: %w", err)
		}
	}

	bldr := array.NewRecordBuilder(s.mem, outSchema)
	defer bldr.Release()

	catalogBldr := bldr.Field(0).(*array.StringBuilder)
	dbSchemaBldr := bldr.Field(1).(*array.StringBuilder)
	tableNameBldr := bldr.Field(2).(*array.StringBuilder)
	tableTypeBldr := bldr.Field(3).(*array.StringBuilder)

	var tableSchemaBldr *array.BinaryBuilder
	if cmd.GetIncludeSchema() {
		tableSchemaBldr = bldr.Field(4).(*array.BinaryBuilder)
	}

	for _, name := range s.cat.Tables() {
		if nameFilter != nil && !nameFilter.MatchString(name) {
			continue
		}

		catalogBldr.AppendNull()
		dbSchemaBldr.AppendNull()
		tableNameBldr.Append(name)
		tableTypeBldr.Append("TABLE")

		if tableSchemaBldr != nil {
			tblSchema, err := s.schemaForQuery(ctx, fmt.Sprintf("SELECT * FROM %q", name))
			if err != nil {
				return nil, nil, fmt.Errorf("cannot infer schema for table %q: %w", name, err)
			}
			tableSchemaBldr.Append(flight.SerializeSchema(tblSchema, s.mem))
		}
	}

	record := bldr.NewRecordBatch()
	ch := make(chan flight.StreamChunk, 1)
	ch <- flight.StreamChunk{Data: record}
	close(ch)

	return outSchema, ch, nil
}

// TODO: DoGetDBSchemas / GetFlightInfoSchemas
// TODO: DoGetTables / GetFlightInfoTables  (s.cat.Tables())
// TODO: DoGetTableTypes / GetFlightInfoTableTypes
// TODO: DoGetSqlInfo / GetFlightInfoSqlInfo
