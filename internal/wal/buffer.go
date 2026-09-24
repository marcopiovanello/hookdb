package wal

import "sync"

// shared buffer pool for the protobuf serialization.
// reduces GC pressure from repeated allocations
var bufferPool = sync.Pool{
	New: func() any {
		b := make([]byte, 0, 1024)
		return &b
	},
}
