// Package server implementa la superficie Arrow Flight SQL del motore.
//
// IMPORTANTE: le firme esatte dei metodi di flightsql.BaseServer possono
// variare leggermente da una versione all'altra di github.com/apache/arrow-go.
// Questo file è stato scritto seguendo il pattern dell'esempio ufficiale
// (arrow-go/arrow/flight/flightsql/example, backend SQLite) ma NON è stato
// compilato in questo ambiente (qui non ho accesso al module proxy Go).
// Prima di fidartene alla lettera:
//
//  1. `go get github.com/apache/arrow-go/v18`
//  2. `go doc github.com/apache/arrow-go/v18/arrow/flight/flightsql BaseServer`
//  3. Confronta le firme con quelle usate qui e correggi eventuali scostamenti.
//
// I metodi core (GetFlightInfoStatement / DoGetStatement, cioè "esegui
// questa SQL e restituiscimi i dati") sono la parte più importante e più
// stabile dell'API. I metodi di metadata (cataloghi/schemi/tabelle) sono
// lasciati come stub con TODO: senza di essi l'esecuzione di query dirette
// funziona comunque, ma l'autocomplete/schema-browser di Grafana potrebbe
// non popolarsi. Per completarli, il modo più rapido è copiare le relative
// implementazioni dall'esempio SQLite ufficiale e sostituire l'accesso a
// SQLite con query equivalenti su DuckDB (information_schema / pragma_*).
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

func (s *Server) GetFlightInfoStatement(ctx context.Context, cmd flightsql.StatementQuery, desc *flight.FlightDescriptor) (*flight.FlightInfo, error) {
	query := cmd.GetQuery()

	schema, err := s.schemaForQuery(ctx, query)
	if err != nil {
		return nil, fmt.Errorf("impossibile determinare lo schema della query: %w", err)
	}

	ticketBytes, err := flightsql.CreateStatementQueryTicket([]byte(query))
	if err != nil {
		return nil, fmt.Errorf("creazione ticket: %w", err)
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

func (s *Server) DoGetStatement(ctx context.Context, ticket flightsql.StatementQueryTicket) (*arrow.Schema, <-chan flight.StreamChunk, error) {
	query := string(ticket.GetStatementHandle())

	rows, err := s.db.QueryContext(ctx, query)
	if err != nil {
		return nil, nil, fmt.Errorf("esecuzione query: %w", err)
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
// TODO: DoGetTables / GetFlightInfoTables  (usa s.cat.Tables())
// TODO: DoGetTableTypes / GetFlightInfoTableTypes
// TODO: DoGetSqlInfo / GetFlightInfoSqlInfo
