package pool

import (
	"context"
	"database/sql/driver"

	"github.com/duckdb/duckdb-go/v2"
)

type DuckDBConnectorPool struct {
	connector *duckdb.Connector
	pool      chan driver.Conn
}

func NewDuckDBConnectorPool(connector *duckdb.Connector, size int) (Pool, error) {
	p := &DuckDBConnectorPool{connector: connector, pool: make(chan driver.Conn, size)}
	for range size {
		conn, err := connector.Connect(context.Background())
		if err != nil {
			return nil, err
		}
		p.pool <- conn
	}
	return p, nil
}

func (p *DuckDBConnectorPool) Acquire(ctx context.Context) (driver.Conn, error) {
	select {
	case c := <-p.pool:
		return c, nil
	case <-ctx.Done():
		return nil, ctx.Err()
	}
}

func (p *DuckDBConnectorPool) Release(c driver.Conn) {
	p.pool <- c
}
