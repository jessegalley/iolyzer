/*
 *
 * jesse galley <jesse@jessegalley.net>
 */

// package iotest abstracts the variou test scenarios that
// can be run with iolyzer. some tests have different options
// and different result sets
package iotest

import (
	"fmt"
	"os"
	"time"

	// "github.com/davecgh/go-spew/spew"
	"github.com/jessegalley/iolyzer/internal/config"
	"github.com/jessegalley/iolyzer/internal/layout"
	"github.com/jessegalley/iolyzer/internal/runners"
	"github.com/jessegalley/iolyzer/internal/stats"
)

type IOTest struct {
	Config *config.Config
}

func New(config *config.Config) (*IOTest, error) {
	return &IOTest{Config: config}, nil
}

func (t *IOTest) StartMixedRW() {
	//TODO: refactor mixedrw test to IOTest arch
}

// StartMDSThrash executes the MDS Thrash test
func (t *IOTest) StartMDSThrash() error {

	// spew.Dump(t.Config)

	// ensure that the test directories exist before proceeding
	if err := layout.ValidateMDSThrashDirectories(t.Config.TestDir, t.Config.DirCount); err != nil {
		fmt.Fprintf(os.Stderr, "test directories don't exist, creating them\n")
		err = layout.CreateMDSThrashDirectories(t.Config.TestDir, t.Config.DirCount)
		if err != nil {
			return fmt.Errorf("couldn't create test dirs: %v", err)
		}
	}

	// set up stats collector
	collector := stats.NewStatsCollector(100, 10, true)

	// create display with configuration
	displayConfig := stats.DisplayConfig{
		UpdateInterval: 500 * time.Millisecond,
		ShowLatency:    true,
		ShowProgress:   true,
		TestDuration:   t.Config.TestDuration,
		Quiet:          false,
	}
	display := stats.NewStatsDisplay(collector, displayConfig)

	// start both collector and display
	collector.Start()
	display.Start()

	// time.Sleep(t.Config.TestDuration)
	runners.RunMDSThrash(t.Config, collector)
	// run test here...
	// workers would be sending updates to collector
	// display automatically shows live updates

	// when test completes
	collector.Stop()
	display.Stop()

	// show final summary
	finalStats := collector.GetFinalStats()
	display.ShowFinalSummary(finalStats)

	// fmt.Println("started mds thrash test...")
	return nil
}
