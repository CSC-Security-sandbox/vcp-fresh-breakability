package main

import (
	"context"
	"log/slog"
	"os"
	"time"
)

func main() {
	log := slog.Default()
	start := time.Now()
	defer func() { log.Info("total job time", "ms", time.Since(start).Milliseconds()) }()
	os.Exit(run(context.Background(), configFromEnv(), log))
}
