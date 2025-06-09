// mdsthrash.go contains the runner code responsible
// for actually executing the MDS Thrash test
package runners

// TODO: this is going to be created here for the mds thrash test, but  
//       this config struct should be refactored into all tests instead 
//       of func args  
type TestConfig struct {
  ParallelJobs   int // how many child processes to use when doing IO jobs 
  TestDir        string // the root path for the test dir  
  DirCount       int // how many dirs to use for random thrashing
  FileSize       int64  // how many bytes are written to each file in the input dir 
  FilesPerBatch  int // how many small files are created before moving on
  FilesPerMove   int // how many small files are created before they've moved to output 
  FsyncFreq      int // what percentage of writes are fsync()'d afterwards 
  FsyncCloseFreq int // what percentage of the time files are fsync()'d before close() 
  // DangleFreq     int // what percentage of the time an fd is not closed 
  BlockSize      int // buffer size in bytes for all reads/writes 
  MaxReadFiles   int // how many files maximum to read in the unlinking phase of the test  
}

func MDSThrashTest(c TestConfig) (TestResult, error) {

  // this test requires a file tree like the following:
  // <testdir>/{in,out}/{01..NN}
  // where NN is the c.DirCount 
  // ensure that this tree exists, and all dirs are acessible and writable
  // otherwise return some err 

  // the parent goroutine will spawn N child go routines, the children will do the following:
  // writing phase: 
  //   select a random <testdir>/in/{00..NN}
  //   create and open for writing a new file in that directory
  //   the file will be named `{HOSTNAME}.{PID}.{CHILDNO}.{UNIXTS+NANO}`
  //      {CHILDNO} is just a number from 1 to N where N is the number of child goroutines 
  //   then, to this file, random data will be written in BlockSize chunks, until the file is FileSize total 
  //      the writes will be in O_DIRECT or O_SYNC if those arguments were set 
  //      if FsyncFreq is given, fsync() will sometimes be called after writes based on the frquency 
  //      if FsyncCloseFreq is given fsync() will be sometimes be called before the file is closed based on the frquency 
  //   we will continue creating, opening, and writing these files in this dir in a loop until we've created 
  //   FilesPerBatch number of them, at which point they will all be moved (ranamed) to <testdir>/out/NN where NN 
  //   is the same random numberd dir we selected earlier 
  //   one caveat is that if FilesPerMove is set, we may move files before we are done ceating in this dir 
  //   for example if FilesPerBatch is 10, and FilesPerMOve is 2,  every 2 files we will move them to out/NN, while 
  //   continuing to create them in the same dir until FilesPerBatch is reached, then we will move any files  
  //   not already moved and continue on. 
  // reading/unlinking phase:
  //   the child goroutine then randomly selects a different NN dir, but this time in <testdir>/out/NN 
  //   it's important the NN isn't the same NN as was used in input/NN, in order to maximize the chances of 
  //   causing metadata capability conflicts  
  //   the goroutine then lists the files in this directory, and loops over each one, reading bytes from it in 
  //   BlockSize chunks until the whole file is read, then it closes and deletes the file.
  //   once all files that were originally listed are read and closed, or MaxReadFiles have been processed;
  //     go back to the writing phase in another random in/NN dir and repeat the whole process. 
  //     it's important that during the reading/deleteing phase, that the list dents is not run again. list the dents 
  //     once, process the files, move on.

  // counters of each data and metadata operation should be kept, counters of the total bytes read and written should be kept  
  // min, max, avg latency for each operation should be kept.
  
  return TestResult{}, nil 
}

