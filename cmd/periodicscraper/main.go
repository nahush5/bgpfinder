package main

import (
	"flag"
	"fmt"
	"os"
	"time"

	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/alistairking/bgpfinder/periodicscraper"
)

func main() {
	logLevel := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
	envFile := flag.String("env-file", ".env", "Path to .env file (required if use-db is true)")
	ripeRisRibsStartTime := flag.String("ripeRisRibsStartTime", "0000-00-00 00:00:00", "Time stamp dudes")
	ripeRisUpdatesStartTime := flag.String("ripeRisUpdatesStartTime", "0000-00-00 00:00:00", "Time stamp dudes")
	routeViewsRibsStartTime := flag.String("routeViewsRibsStartTime", "0000-00-00 00:00:00", "Time stamp dudes")
	routeViewsUpdatesStartTime := flag.String("routeViewsUpdatesStartTime", "0000-00-00 00:00:00", "Time stamp dudes")
	flag.Parse()

	layout := "2006-01-02 15:04:05"
	ripeRisRibsStartTimestamp, err1 := time.Parse(layout, *ripeRisRibsStartTime)
	ripeRisUpdatesStartTimestamp, err2 := time.Parse(layout, *ripeRisUpdatesStartTime)
	routeViewsRibsStartTimestamp, err3 := time.Parse(layout, *routeViewsRibsStartTime)
	routeViewsUpdatesStartTimestamp, err4 := time.Parse(layout, *routeViewsUpdatesStartTime)

	fmt.Println("Envfile: ", *envFile)

	logger := setupLogger(logLevel)
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		logger.Error().Err(err1).Err(err2).Err(err3).Err(err4).Msg("Following error(s) have occurred.")
	}

	periodicscraper.Start(logger, envFile, ripeRisRibsStartTimestamp, ripeRisUpdatesStartTimestamp, routeViewsRibsStartTimestamp, routeViewsUpdatesStartTimestamp)
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
