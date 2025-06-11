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

	"github.com/jessegalley/iolyzer/internal/layout"
)

type IOTest struct {
	Config *Config
}

func New(config *Config) (*IOTest, error) {
	return &IOTest{Config: config}, nil
}

func (t *IOTest) StartMixedRW() {
	//TODO: refactor mixedrw test to IOTest arch
}

// StartMDSThrash executes the MDS Thrash test
func (t *IOTest) StartMDSThrash() error {

	// ensure that the test directories exist before proceeding
	if err := layout.ValidateMDSThrashDirectories(t.Config.TestDir, t.Config.DirCount); err != nil {
		fmt.Fprintf(os.Stderr, "test directories don't exist, creating them\n")
		err = layout.CreateMDSThrashDirectories(t.Config.TestDir, t.Config.DirCount)
		if err != nil {
			return fmt.Errorf("couldn't create test dirs: %v", err)
		}
	}

	// set up stats collector
	sc := NewStatsCollector(128, 128, true)
	sc.Start()

	// set up display?
	// run workers
	// display totals

	fmt.Println("started mds thrash test...")
	return nil
}
