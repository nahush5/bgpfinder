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
	args := os.Args

	if len(args) != 5 {
		fmt.Println("Argument 1: ripeRisRibsStartTime")
		fmt.Println("Argument 2: ripeRisUpdatesStartTime")
		fmt.Println("Argument 3: routeViewsRibsStartTime")
		fmt.Println("Argument 4: routeViewsUpdatesStartTime")
		fmt.Println("The format for time is similar to this 2006-01-02 15:04:05")
		return
	}
	layout := "2006-01-02 15:04:05"
	ripeRisRibsStartTime, err1 := time.Parse(layout, args[1])
	ripeRisUpdatesStartTime, err2 := time.Parse(layout, args[2])
	routeViewsRibsStartTime, err3 := time.Parse(layout, args[3])
	routeViewsUpdatesStartTime, err4 := time.Parse(layout, args[4])

	logger := setupLogger()
	if err1 != nil || err2 != nil || err3 != nil || err4 != nil {
		logger.Error().Err(err1).Err(err2).Err(err3).Err(err4).Msg("Following error(s) have occurred.")
	}

	periodicscraper.Start(logger, ripeRisRibsStartTime, ripeRisUpdatesStartTime, routeViewsRibsStartTime, routeViewsUpdatesStartTime)
}

func setupLogger() *logging.Logger {
	logLevel := flag.String("loglevel", "info", "Log level (debug, info, warn, error)")
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
