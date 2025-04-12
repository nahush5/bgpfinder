package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/alistairking/bgpfinder/periodicscraper"
)

func main() {
	logLevel := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	envFile := flag.String("env-file", ".env", "Path to .env file (required if use-db is true)")
	flag.Parse()

	logger := setupLogger(logLevel)

	periodicscraper.Start(logger, envFile)
}

func setupLogger(logLevel *string) *logging.Logger {
	loggerConfig := logging.LoggerConfig{
		LogLevel: *logLevel,
	}
	logger, err := logging.NewLogger(loggerConfig)
	if err != nil {
		fmt.Fprintf(os.Stderr, "Failed to initialize logger: %v\n", err)
		os.Exit(1)
	}
	return logger
}
