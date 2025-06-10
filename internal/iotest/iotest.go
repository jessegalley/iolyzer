/*
 *
 * jesse galley <jesse@jessegalley.net>
 */

// package iotest abstracts the variou test scenarios that
// can be run with iolyzer. some tests have different options
// and different result sets
package iotest

type IOTest struct {
	TestDir      string
	ParallelJobs int

	debug int
}

func (t *IOTest) StartMixedRW() {

}

func (t *IOTest) StartMDSThrash() {

}
