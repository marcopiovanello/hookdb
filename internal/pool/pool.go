package pool

import (
	"context"
	"database/sql/driver"
)

type Pool interface {
	Acquire(ctx context.Context) (driver.Conn, error)
	Release(c driver.Conn)
}
