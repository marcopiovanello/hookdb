package main

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"flag"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/duckdb/duckdb-go/v2"
	_ "github.com/duckdb/duckdb-go/v2"
	"google.golang.org/grpc"

	"github.com/apache/arrow-go/v18/arrow/flight"
	"github.com/apache/arrow-go/v18/arrow/flight/flightsql"

	"github.com/marcopiovanello/hookdb/internal/catalog"
	"github.com/marcopiovanello/hookdb/internal/compaction"
	"github.com/marcopiovanello/hookdb/internal/config"
	"github.com/marcopiovanello/hookdb/internal/pool"
	"github.com/marcopiovanello/hookdb/internal/server"

	"github.com/improbable-eng/grpc-web/go/grpcweb"
)

var (
	configFilePath string
)

func main() {
	flag.StringVar(&configFilePath, "c", "config.yaml", "config file path to merge with defaults")
	flag.Parse()

	logger := slog.New(slog.NewTextHandler(os.Stdout, nil))

	cfg := config.LoadOrDefaults(configFilePath)

	mainCtx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()

	// in memory duckdb for query execution and container for the views (which are rebuilt on restart)
	connector, err := duckdb.NewConnector(":memory:", func(execer driver.ExecerContext) error { return nil })
	if err != nil {
		logger.Error("failed creating duckdb connector", "err", err)
		os.Exit(1)
	}

	db := sql.OpenDB(connector)
	defer db.Close()

	cat := catalog.New(db, cfg.DataDir, cfg.DeleteGracePeriod, logger)
	if err := cat.Scan(); err != nil {
		logger.Error("failed initial catalog scan", "err", err)
		os.Exit(1)
	}
	defer cat.Close()

	// parquet files discovery in data dir
	go runTicker(mainCtx, cfg.ScanInterval, func() {
		if err := cat.Scan(); err != nil {
			logger.Error("failed catalog scan", "err", err)
		}
	})

	compactor := compaction.New(&compaction.CompactorArgs{
		Catalog:    cat,
		Database:   db,
		Interval:   cfg.CompactionInterval,
		Levels:     cfg.Levels,
		TimeColumn: cfg.TimeColumn,
		Logger:     logger,
	})
	go compactor.Run(mainCtx)

	// arrow flight SQL server used as efficient columnar data transfer protocol
	duckdbPool, err := pool.NewDuckDBConnectorPool(connector, 8)
	if err != nil {
		logger.Error("failed creating duckdb connector pool", "err", err)
		os.Exit(1)
	}

	impl := server.New(duckdbPool, cat, logger)
	flightSrv := flightsql.NewFlightServer(impl)

	grpcServer := grpc.NewServer()
	defer grpcServer.GracefulStop()

	flight.RegisterFlightServiceServer(grpcServer, flightSrv)

	wrappedGrpc := grpcweb.WrapServer(grpcServer,
		grpcweb.WithOriginFunc(func(origin string) bool { return true }), //TODO: configure CORS
	)

	handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		if wrappedGrpc.IsGrpcWebRequest(r) || wrappedGrpc.IsAcceptableGrpcCorsRequest(r) {
			wrappedGrpc.ServeHTTP(w, r)
			return
		}
		if r.ProtoMajor == 2 && r.Header.Get("Content-Type") == "application/grpc" {
			grpcServer.ServeHTTP(w, r)
			return
		}
		w.WriteHeader(http.StatusOK)
	})

	httpServer := &http.Server{
		Addr:    cfg.ListenAddr,
		Handler: handler,
	}
	defer httpServer.Close()

	httpServer.Protocols = new(http.Protocols)
	httpServer.Protocols.SetUnencryptedHTTP2(true) //TODO: enable public key authentication

	logger.Info("grpc server listening", "addr", cfg.ListenAddr, "data_dir", cfg.DataDir)

	go func() {
		if err := httpServer.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			logger.Error("server http/grpc stopped", "err", err)
			os.Exit(1)
		}
	}()

	<-mainCtx.Done()

	logger.Info("shutting down...")
}

func runTicker(ctx context.Context, interval time.Duration, fn func()) {
	if interval <= 0 {
		interval = 5 * time.Second
	}

	ticker := time.NewTicker(interval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			fn()
		}
	}
}
