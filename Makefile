default:
	GOAMD64=v3 CGO_ENABLED=1 go build -tags=duckdb_arrow -o hookdb ./cmd/hookdb/main.go

protobuf:
	protoc --go_out=./internal/wal --go_opt=module=github.com/marcopiovanello/hookdb/internal/wal ./proto/wal.proto