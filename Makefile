protobuf:
	protoc --go_out=./internal/wal --go_opt=module=github.com/marcopiovanello/hookdb/internal/wal ./proto/wal.proto