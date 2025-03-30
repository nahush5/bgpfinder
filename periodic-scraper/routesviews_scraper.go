package periodicscraper

import "github.com/alistairking/bgpfinder"

func routeViewsDriver(isRibs bool) {
	ctx := setupContext()
	logger := setupLogger()
	db := setupDB(logger)

	collectors, prevRuntimes, err := runDb(ctx, logger, db, ROUTEVIEWS)

	if err != nil {
		return
	}

	PeriodicScraper(ctx, logger, getRetryInterval(ROUTEVIEWS, isRibs), prevRuntimes, collectors, db, bgpfinder.NewRouteViewsFinder(), isRibs)
}
