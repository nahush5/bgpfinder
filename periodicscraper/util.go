package periodicscraper

import (
	"context"
	"fmt"
	"time"

	"github.com/alistairking/bgpfinder"
	"github.com/alistairking/bgpfinder/internal/logging"
	"github.com/jackc/pgx/v5/pgxpool"
)

const (
	RIS                      = "ris"
	ROUTEVIEWS               = "routeviews"
	routeviewRibsInterval    = (2*60 + 0) * 60
	routeviewUpdatesInterval = (0*60 + 15) * 60
	risRibsInterval          = (8*60 + 0) * 60
	risUpdatesInterval       = (0*60 + 5) * 60
	divVal                   = 64
)

func runDb(ctx context.Context,
	logger *logging.Logger,
	db *pgxpool.Pool,
	project string) ([]bgpfinder.Collector, []time.Time, error) {

	stmt := `SELECT name, last_completed_crawl_time FROM collectors where project_name = $1`

	rows, err := db.Query(ctx, stmt, project)
	if err != nil {
		logger.Error().Err(err).Msg("Query failed, continuing execution")
		return nil, nil, err
	}
	defer rows.Close()

	// Iterate over rows
	var collectors []bgpfinder.Collector
	var timeArray []time.Time

	for rows.Next() {
		var collectorName string
		var collector bgpfinder.Collector
		var lastCompletedCrawlTime time.Time // Use time.Time if it's a timestamp

		err := rows.Scan(&collectorName, &lastCompletedCrawlTime)
		if err != nil {
			logger.Error().Err(err).Msg("Scan failed")
		}
		collector.Project.Name = project
		collector.Name = collectorName

		fmt.Printf("Collector: %s, Last Completed Crawl Time: %s\n", collectorName, lastCompletedCrawlTime)
		collectors = append(collectors, collector)
		timeArray = append(timeArray, lastCompletedCrawlTime)
	}

	// Check for errors after iteration
	if err := rows.Err(); err != nil {
		logger.Error().Err(err).Msg("Row iteration error")
	}

	return collectors, timeArray, nil
}

func getRetryInterval(project string, isRibs bool) int64 {
	switch project {
	case ROUTEVIEWS:
		if isRibs {
			return routeviewRibsInterval / divVal
		} else {
			return routeviewUpdatesInterval / divVal
		}
	case RIS:
		if isRibs {
			return risRibsInterval / divVal
		} else {
			return risUpdatesInterval / divVal
		}
	}
	return 0
}
