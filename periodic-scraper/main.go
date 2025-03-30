package periodicscraper

import (
	"fmt"
	"os"
	"strconv"
)

func main() {
	args := os.Args

	// Check if there are enough arguments
	if len(args) != 3 {
		fmt.Println("Usage: <program> <project-name> <is-ribs>")
		return
	}

	isRibs, err := strconv.ParseBool(args[2])

	if err != nil {
		fmt.Println("Error parsing ribs/updates argument:", err)
		return
	}

	switch args[1] {
	case ROUTEVIEWS:
		routeViewsDriver(isRibs)
		break
	case RIS:
		risDriver(isRibs)
		break
	default:
		fmt.Println("Incorrect project argument: %s", args[1])
	}
}
