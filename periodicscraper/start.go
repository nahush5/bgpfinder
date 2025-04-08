package periodicscraper

import (
	"context"
	"sync"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

func Start(logger *logging.Logger, envFile *string, ripeRisRibsStartTime time.Time, ripeRisUpdatesStartTime time.Time, routeViewsRibsStartTime time.Time, routeViewsUpdatesStartTime time.Time) {
	db := setupDB(logger, envFile)
	defer db.Close()
	ctx, stop := setupContext()
	defer stop()

	logger.Info().Msg("Starting runn")

	var wg sync.WaitGroup

	// Start each scraping task in its own goroutine
	wg.Add(1)
	go func() {
		defer wg.Done()
		startScraping(ctx, logger, db, RIS, true, risRibsInterval, ripeRisRibsStartTime)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		startScraping(ctx, logger, db, RIS, false, risUpdatesInterval, ripeRisUpdatesStartTime)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		startScraping(ctx, logger, db, ROUTEVIEWS, true, routeviewRibsInterval, routeViewsRibsStartTime)
	}()

	wg.Add(1)
	go func() {
		defer wg.Done()
		startScraping(ctx, logger, db, ROUTEVIEWS, false, routeviewUpdatesInterval, routeViewsUpdatesStartTime)
	}()

	// Wait for all scraping tasks to complete
	wg.Wait()

	<-ctx.Done() // Wait for context cancellation

	logger.Info().Msg("Exiting Main()")
}

func startScraping(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	project string,
	isRibsData bool,
	frequency int64, waitTill time.Time) {
	waitUntilTimestamp(waitTill)
	logger.Info().Msgf("Reached target time: %v", waitTill)
	for {
		if ctx.Err() != nil {
			return
		}
		startTime := time.Now()
		driver(ctx, logger, db, project, isRibsData)
		elapsedTime := time.Since(startTime)
		logger.Info().Msgf("Scraping runtime for project %s and isribs %t is %v", project, isRibsData, elapsedTime)
		time.Sleep(time.Duration(frequency) * time.Second) // or <-tick.C
	}
}

func waitUntilTimestamp(targetTime time.Time) {
	duration := time.Until(targetTime)

	if duration <= 0 {
		return
	}

	time.Sleep(duration)
}

func driver(ctx context.Context, logger *logging.Logger, db *pgxpool.Pool, collector string, isRibs bool) {
	logger.Info().Msgf("Starting periodic collectors data for %s isribs: %t", collector, isRibs)
	collectors, prevRuntimes, err := getCollectorsAndPrevRuntime(ctx, logger, db, collector)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run db to collect data for %s isribs: %t data for collectors", collector, isRibs)
	} else {
		logger.Info().Msgf("Run of db %s isribs: %t completed successfully", collector, isRibs)
	}
	err = PeriodicScraper(ctx, logger, getRetryInterval(collector, isRibs), prevRuntimes, collectors, db, bgpfinder.NewRouteViewsFinder(), isRibs)
	if err != nil {
		logger.Error().Err(err).Msgf("Failed to run periodic scraper %s isribs: %t data for collectors", collector, isRibs)
	} else {
		logger.Info().Msgf("Run of periodic scraper %s isribs: %t completed successfully", collector, isRibs)
	}
}
