/*
 * 
 * jesse galley <jesse@jessegalley.net>
 */ 

// package iotest abstracts the variou test scenarios that 
// can be run with iolyzer. some tests have different options 
// and different result sets
package iotest


type IOTest struct { 
  TestDir       string
  ParallelJobs  int

}

// New constructs a new instance of IOTest, with functional options 
// for any of the runtime CLI opts.
func New(options ...func(*IOTest)) *IOTest {
  t := &IOTest{}
  for _, o := range options {
    o(t)
  }

  return t 
}



func (t *IOTest) StartMixedRW() {

}

func (t *IOTest) StartMDSThrash() {

}


