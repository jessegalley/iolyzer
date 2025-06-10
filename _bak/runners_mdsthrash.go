package runners

import (
	"crypto/rand"
	"fmt"
	// "io"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// TestConfig holds configuration for the MDS thrash test
// this config struct should be refactored into all tests instead
// of func args but for now we'll use it specifically for mdsthrash
type TestConfig struct {
	// how many child processes to use when doing IO jobs
	ParallelJobs int

	// the root path for the test dir
	TestDir string

	// how many dirs to use for random thrashing
	DirCount int

	// how many bytes are written to each file in the input dir
	FileSize int64

	// how many small files are created before moving on
	FilesPerBatch int

	// how many small files are created before they've moved to output
	FilesPerMove int

	// what percentage of writes are fsync()'d afterwards
	FsyncFreq int

	// what percentage of the time files are fsync()'d before close()
	FsyncCloseFreq int

	// buffer size in bytes for all reads/writes
	BlockSize int

	// how many files maximum to read in the unlinking phase of the test
	MaxReadFiles int

	// duration to run the test
	Duration time.Duration
}

// NewTestConfig creates a new TestConfig with sensible defaults
func NewTestConfig() TestConfig {
	return TestConfig{
		ParallelJobs:   4,             // reasonable default for most systems
		TestDir:        "./mdsthrash", // default test directory
		DirCount:       8,             // moderate number of directories
		FileSize:       4096,          // 4KB files by default
		FilesPerBatch:  10,            // create 10 files before moving on
		FilesPerMove:   0,             // move all files at end of batch by default
		FsyncFreq:      0,             // no fsync by default
		FsyncCloseFreq: 0,             // no fsync before close by default
		BlockSize:      4096,          // 4KB block size
		MaxReadFiles:   0,             // unlimited reads by default
		Duration:       time.Minute,   // 1 minute default test duration
	}
}

// MDSThrashResult contains detailed metrics from the MDS thrash test
type MDSThrashResult struct {
	// basic operation counts
	CreateCount int64
	WriteCount  int64
	ReadCount   int64
	MoveCount   int64
	UnlinkCount int64

	// error counts
	CreateErrors int64
	WriteErrors  int64
	ReadErrors   int64
	MoveErrors   int64
	UnlinkErrors int64

	// byte transfer metrics
	BytesRead    int64
	BytesWritten int64

	// latency metrics for each operation type
	CreateLatency LatencyStats
	WriteLatency  LatencyStats
	ReadLatency   LatencyStats
	MoveLatency   LatencyStats
	UnlinkLatency LatencyStats

	// test duration
	Duration time.Duration
}

// LatencyStats holds min, max, and average latency for an operation type
type LatencyStats struct {
	Min   time.Duration
	Max   time.Duration
	Avg   time.Duration
	Count int64
}

// updateLatency updates latency statistics with a new measurement
func (ls *LatencyStats) updateLatency(latency time.Duration) {
	// increment operation count
	ls.Count++

	// update min latency
	if ls.Min == 0 || latency < ls.Min {
		ls.Min = latency
	}

	// update max latency
	if latency > ls.Max {
		ls.Max = latency
	}

	// calculate running average
	// use simple incremental average calculation
	ls.Avg = time.Duration((int64(ls.Avg)*(ls.Count-1) + int64(latency)) / ls.Count)
}

// workerStats holds per-worker statistics that get aggregated
type workerStats struct {
	sync.Mutex

	// operation counts
	createCount int64
	writeCount  int64
	readCount   int64
	moveCount   int64
	unlinkCount int64
	fsyncCount  int64

	// byte counts
	bytesRead    int64
	bytesWritten int64

	// error counts
	createErrors int64
	writeErrors  int64
	readErrors   int64
	moveErrors   int64
	unlinkErrors int64
	fsyncErrors  int64

	// latency tracking
	createLatency LatencyStats
	writeLatency  LatencyStats
	readLatency   LatencyStats
	moveLatency   LatencyStats
	unlinkLatency LatencyStats
	fsyncLatency  LatencyStats
}

// MDSThrashTest performs the MDS thrash test according to the provided configuration
// returns a basic TestResult for compatibility with existing output formatters
func MDSThrashTest(c TestConfig) (TestResult, error) {
	// get detailed results
	detailedResult, err := MDSThrashTestDetailed(c)
	if err != nil {
		return TestResult{}, err
	}

	// convert to basic TestResult for compatibility
	testResult := TestResult{
		ReadCount:    detailedResult.ReadCount,
		WriteCount:   detailedResult.WriteCount,
		BytesRead:    detailedResult.BytesRead,
		BytesWritten: detailedResult.BytesWritten,
		Duration:     detailedResult.Duration,
	}

	return testResult, nil
}

// MDSThrashTestDetailed performs the MDS thrash test and returns detailed results
func MDSThrashTestDetailed(c TestConfig) (MDSThrashResult, error) {
	// create the required directory structure
	// this test requires a file tree like the following:
	// <testdir>/{in,out}/{01..NN} where NN is the c.DirCount
	err := createMDSThrashDirectories(c.TestDir, c.DirCount)
	if err != nil {
		return MDSThrashResult{}, fmt.Errorf("failed to create directory structure: %w", err)
	}

	// create channel for collecting worker results
	resultsChan := make(chan *workerStats, c.ParallelJobs)

	// create wait group for worker synchronization
	var wg sync.WaitGroup

	// record test start time
	startTime := time.Now()

	// launch worker goroutines
	// the parent goroutine will spawn N child go routines
	for i := 0; i < c.ParallelJobs; i++ {
		wg.Add(1)

		// create worker-specific random number generator
		// use unique seed to ensure different random sequences per worker
		workerRng := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(i)))

		// launch worker in separate goroutine
		go func(workerID int, rng *mathrand.Rand) {
			defer wg.Done()

			// run the worker and collect its statistics
			stats, err := mdsThrashWorker(c, workerID, rng, startTime)
			if err != nil {
				fmt.Printf("worker %d error: %v\n", workerID, err)
				return
			}

			// send results back to main goroutine
			resultsChan <- stats
		}(i, workerRng)
	}

	// wait for all workers to complete
	wg.Wait()

	// close results channel
	close(resultsChan)

	// aggregate results from all workers
	aggregateResult := aggregateWorkerResults(resultsChan, time.Since(startTime))

	return aggregateResult, nil
}

// createMDSThrashDirectories creates the required directory structure for the test
// ensures that the tree <testdir>/{in,out}/{01..NN} exists and is accessible
func createMDSThrashDirectories(testDir string, dirCount int) error {
	// create base directories for input and output
	inDir := filepath.Join(testDir, "in")
	outDir := filepath.Join(testDir, "out")

	// create input and output base directories
	err := os.MkdirAll(inDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create input directory: %w", err)
	}

	err = os.MkdirAll(outDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// create numbered subdirectories in both input and output
	for i := 1; i <= dirCount; i++ {
		// create input subdirectory
		inSubDir := filepath.Join(inDir, fmt.Sprintf("%02d", i))
		err = os.MkdirAll(inSubDir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create input subdirectory %s: %w", inSubDir, err)
		}

		// create output subdirectory
		outSubDir := filepath.Join(outDir, fmt.Sprintf("%02d", i))
		err = os.MkdirAll(outSubDir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create output subdirectory %s: %w", outSubDir, err)
		}

		// verify directories are writable by attempting to create a test file
		testFile := filepath.Join(inSubDir, ".write_test")
		f, err := os.Create(testFile)
		if err != nil {
			return fmt.Errorf("input directory %s is not writable: %w", inSubDir, err)
		}
		f.Close()
		os.Remove(testFile)

		testFile = filepath.Join(outSubDir, ".write_test")
		f, err = os.Create(testFile)
		if err != nil {
			return fmt.Errorf("output directory %s is not writable: %w", outSubDir, err)
		}
		f.Close()
		os.Remove(testFile)
	}

	return nil
}

// mdsThrashWorker performs the actual MDS thrashing operations for a single worker
func mdsThrashWorker(config TestConfig, workerID int, rng *mathrand.Rand, startTime time.Time) (*workerStats, error) {

	fmt.Println("DEBUG worker started", workerID)
	// initialize worker statistics
	stats := &workerStats{}

	// get hostname and process ID for unique file naming
	hostname, err := os.Hostname()
	if err != nil {
		hostname = "unknown"
	}
	pid := os.Getpid()

	// create write buffer filled with random data
	writeBuffer := make([]byte, config.BlockSize)
	_, err = rand.Read(writeBuffer)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random write data: %w", err)
	}

	// create read buffer for file operations
	readBuffer := make([]byte, config.BlockSize)

	// main worker loop - continue until test duration expires
	for time.Since(startTime) < config.Duration {
		// writing phase: create files in random input directory
		err := performWritingPhase(config, workerID, hostname, pid, rng, stats, writeBuffer)
		if err != nil {
			return nil, fmt.Errorf("writing phase failed: %w", err)
		}

		// reading/unlinking phase: read and delete files from random output directory
		err = performReadingPhase(config, workerID, rng, stats, readBuffer)
		if err != nil {
			return nil, fmt.Errorf("reading phase failed: %w", err)
		}

		// check if test duration has expired
		if time.Since(startTime) >= config.Duration {
			break
		}
	}

	return stats, nil
}

// performWritingPhase handles the file creation and writing portion of the test
func performWritingPhase(config TestConfig, workerID int, hostname string, pid int, rng *mathrand.Rand, stats *workerStats, writeBuffer []byte) error {
	// select a random input directory for this batch
	inputDirNum := rng.Intn(config.DirCount) + 1
	inputDir := filepath.Join(config.TestDir, "in", fmt.Sprintf("%02d", inputDirNum))

	fmt.Println("DEBUG: starting write phase worker: ", workerID)

	// track files created in this batch for moving
	var createdFiles []string

	// create FilesPerBatch number of files
	for fileCount := 0; fileCount < config.FilesPerBatch; fileCount++ {
		// generate unique filename using the specified format
		// {HOSTNAME}.{PID}.{CHILDNO}.{UNIXTS+NANO}
		timestamp := time.Now().UnixNano()
		filename := fmt.Sprintf("%s.%d.%d.%d", hostname, pid, workerID, timestamp)
		filepath := filepath.Join(inputDir, filename)

		// create and write to the file
		err := createAndWriteFile(filepath, config, stats, writeBuffer)
		if err != nil {
			// count the error but continue with other files
			stats.Lock()
			stats.createErrors++
			stats.Unlock()
			continue
		}

		// add to list of files to be moved
		createdFiles = append(createdFiles, filepath)

		// check if we should move files based on FilesPerMove setting
		if config.FilesPerMove > 0 && (fileCount+1)%config.FilesPerMove == 0 {
			// move the accumulated files to output directory
			err := moveFilesToOutput(createdFiles, inputDirNum, config.TestDir, stats)
			if err != nil {
				// count the error but continue
				stats.Lock()
				stats.moveErrors++
				stats.Unlock()
			}

			// clear the list since files have been moved
			createdFiles = nil
		}
	}

	// move any remaining files that weren't moved due to FilesPerMove batching
	if len(createdFiles) > 0 {
		err := moveFilesToOutput(createdFiles, inputDirNum, config.TestDir, stats)
		if err != nil {
			// count the error but continue
			stats.Lock()
			stats.moveErrors++
			stats.Unlock()
		}
	}

	return nil
}

// createAndWriteFile creates a single file and writes the specified amount of data
func createAndWriteFile(filepath string, config TestConfig, stats *workerStats, writeBuffer []byte) error {
	// measure file creation latency
	createStart := time.Now()

	fmt.Println("DEBUG: starting create write ")
	// create the file
	file, err := os.Create(filepath)
	if err != nil {
		return fmt.Errorf("failed to create file: %w", err)
	}

	// update creation statistics
	stats.Lock()
	stats.createCount++
	stats.createLatency.updateLatency(time.Since(createStart))
	stats.Unlock()

	// ensure file is closed when function returns
	defer func() {
		// optionally fsync before close based on FsyncCloseFreq
		if config.FsyncCloseFreq > 0 {
			// use random number to determine if we should fsync
			if mathrand.Intn(100) < config.FsyncCloseFreq {
				file.Sync()
			}
		}
		file.Close()
	}()

	// write data to the file in BlockSize chunks until FileSize is reached
	bytesRemaining := config.FileSize

	for bytesRemaining > 0 {
		// determine how much to write in this iteration
		writeSize := int64(len(writeBuffer))
		if writeSize > bytesRemaining {
			writeSize = bytesRemaining
		}

		// measure write latency
		writeStart := time.Now()

		// perform the write operation
		n, err := file.Write(writeBuffer[:writeSize])
		if err != nil {
			stats.Lock()
			stats.writeErrors++
			stats.Unlock()
			return fmt.Errorf("failed to write data: %w", err)
		}

		// update write statistics
		stats.Lock()
		stats.writeCount++
		stats.bytesWritten += int64(n)
		stats.writeLatency.updateLatency(time.Since(writeStart))
		stats.Unlock()

		// update remaining bytes
		bytesRemaining -= int64(n)

		// optionally perform fsync based on FsyncFreq
		if config.FsyncFreq > 0 {
			stats.Lock()
			shouldFsync := stats.writeCount%int64(config.FsyncFreq) == 0
			stats.Unlock()

			if shouldFsync {
				err = file.Sync()
				if err != nil {
					return fmt.Errorf("failed to fsync: %w", err)
				}
			}
		}
	}

	return nil
}

// moveFilesToOutput moves a list of files from input directory to corresponding output directory
func moveFilesToOutput(files []string, inputDirNum int, testDir string, stats *workerStats) error {
	// determine the output directory path
	outputDir := filepath.Join(testDir, "out", fmt.Sprintf("%02d", inputDirNum))

	// move each file to the output directory
	for _, srcPath := range files {
		// extract filename from source path
		filename := filepath.Base(srcPath)
		dstPath := filepath.Join(outputDir, filename)

		// measure move operation latency
		moveStart := time.Now()

		// perform the move operation using rename
		err := os.Rename(srcPath, dstPath)
		if err != nil {
			stats.Lock()
			stats.moveErrors++
			stats.Unlock()
			continue
		}

		// update move statistics
		stats.Lock()
		stats.moveCount++
		stats.moveLatency.updateLatency(time.Since(moveStart))
		stats.Unlock()
	}

	return nil
}

// performReadingPhase handles the file reading and unlinking portion of the test
func performReadingPhase(config TestConfig, workerID int, rng *mathrand.Rand, stats *workerStats, readBuffer []byte) error {
	// select a random output directory different from the ones used in writing
	// this maximizes the chances of causing metadata capability conflicts
	outputDirNum := rng.Intn(config.DirCount) + 1
	outputDir := filepath.Join(config.TestDir, "out", fmt.Sprintf("%02d", outputDirNum))

	// list files in the selected output directory
	entries, err := os.ReadDir(outputDir)
	if err != nil {
		// if directory doesn't exist or can't be read, skip this phase
		return nil
	}

	// filter to get only regular files and limit by MaxReadFiles
	var filesToProcess []string
	for _, entry := range entries {
		if !entry.IsDir() {
			filesToProcess = append(filesToProcess, entry.Name())

			// respect MaxReadFiles limit
			if config.MaxReadFiles > 0 && len(filesToProcess) >= config.MaxReadFiles {
				break
			}
		}
	}

	// process each file: read it completely then delete it
	for _, filename := range filesToProcess {
		filepath := filepath.Join(outputDir, filename)

		// read the entire file
		err := readAndUnlinkFile(filepath, stats, readBuffer)
		if err != nil {
			// count the error but continue with other files
			stats.Lock()
			stats.readErrors++
			stats.Unlock()
			continue
		}
	}

	return nil
}

// readAndUnlinkFile reads a file completely and then deletes it
func readAndUnlinkFile(filepath string, stats *workerStats, readBuffer []byte) error {
	// open the file for reading
	file, err := os.Open(filepath)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}
	defer file.Close()

	// read the file in chunks until EOF
	for {
		// measure read latency
		readStart := time.Now()

		// read data from file
		n, err := file.Read(readBuffer)
		if err != nil {
			if err.Error() == "EOF" {
				// reached end of file, break the loop
				break
			}
			stats.Lock()
			stats.readErrors++
			stats.Unlock()
			return fmt.Errorf("failed to read file: %w", err)
		}

		// update read statistics
		stats.Lock()
		stats.readCount++
		stats.bytesRead += int64(n)
		stats.readLatency.updateLatency(time.Since(readStart))
		stats.Unlock()

		// if we read less than buffer size, we've reached EOF
		if n < len(readBuffer) {
			break
		}
	}

	// close file before unlinking
	file.Close()

	// measure unlink latency
	unlinkStart := time.Now()

	// delete the file
	err = os.Remove(filepath)
	if err != nil {
		stats.Lock()
		stats.unlinkErrors++
		stats.Unlock()
		return fmt.Errorf("failed to unlink file: %w", err)
	}

	// update unlink statistics
	stats.Lock()
	stats.unlinkCount++
	stats.unlinkLatency.updateLatency(time.Since(unlinkStart))
	stats.Unlock()

	return nil
}

// aggregateWorkerResults combines statistics from all workers into a single result
func aggregateWorkerResults(resultsChan <-chan *workerStats, totalDuration time.Duration) MDSThrashResult {
	// initialize aggregate result
	result := MDSThrashResult{
		Duration: totalDuration,
	}

	// process results from each worker
	for workerStats := range resultsChan {
		// aggregate operation counts
		result.CreateCount += workerStats.createCount
		result.WriteCount += workerStats.writeCount
		result.ReadCount += workerStats.readCount
		result.MoveCount += workerStats.moveCount
		result.UnlinkCount += workerStats.unlinkCount

		// aggregate error counts
		result.CreateErrors += workerStats.createErrors
		result.WriteErrors += workerStats.writeErrors
		result.ReadErrors += workerStats.readErrors
		result.MoveErrors += workerStats.moveErrors
		result.UnlinkErrors += workerStats.unlinkErrors

		// aggregate byte counts
		result.BytesRead += workerStats.bytesRead
		result.BytesWritten += workerStats.bytesWritten

		// aggregate latency statistics
		aggregateLatencyStats(&result.CreateLatency, &workerStats.createLatency)
		aggregateLatencyStats(&result.WriteLatency, &workerStats.writeLatency)
		aggregateLatencyStats(&result.ReadLatency, &workerStats.readLatency)
		aggregateLatencyStats(&result.MoveLatency, &workerStats.moveLatency)
		aggregateLatencyStats(&result.UnlinkLatency, &workerStats.unlinkLatency)
	}

	return result
}

// aggregateLatencyStats combines latency statistics from multiple workers
func aggregateLatencyStats(aggregate *LatencyStats, worker *LatencyStats) {
	// skip if worker has no operations
	if worker.Count == 0 {
		return
	}

	// update minimum latency
	if aggregate.Min == 0 || (worker.Min > 0 && worker.Min < aggregate.Min) {
		aggregate.Min = worker.Min
	}

	// update maximum latency
	if worker.Max > aggregate.Max {
		aggregate.Max = worker.Max
	}

	// calculate weighted average for aggregate average
	if aggregate.Count == 0 {
		aggregate.Avg = worker.Avg
	} else {
		totalOps := aggregate.Count + worker.Count
		aggregate.Avg = time.Duration((int64(aggregate.Avg)*aggregate.Count + int64(worker.Avg)*worker.Count) / totalOps)
	}

	// update operation count
	aggregate.Count += worker.Count
}

// mdsthrash.go contains the runner code responsible
// for actually executing the MDS Thrash test
// package runners
//
// // TODO: this is going to be created here for the mds thrash test, but
// //       this config struct should be refactored into all tests instead
// //       of func args
// type TestConfig struct {
//   ParallelJobs   int // how many child processes to use when doing IO jobs
//   TestDir        string // the root path for the test dir
//   DirCount       int // how many dirs to use for random thrashing
//   FileSize       int64  // how many bytes are written to each file in the input dir
//   FilesPerBatch  int // how many small files are created before moving on
//   FilesPerMove   int // how many small files are created before they've moved to output
//   FsyncFreq      int // what percentage of writes are fsync()'d afterwards
//   FsyncCloseFreq int // what percentage of the time files are fsync()'d before close()
//   // DangleFreq     int // what percentage of the time an fd is not closed
//   BlockSize      int // buffer size in bytes for all reads/writes
//   MaxReadFiles   int // how many files maximum to read in the unlinking phase of the test
// }
//
// func MDSThrashTest(c TestConfig) (TestResult, error) {
//
//   // this test requires a file tree like the following:
//   // <testdir>/{in,out}/{01..NN}
//   // where NN is the c.DirCount
//   // ensure that this tree exists, and all dirs are acessible and writable
//   // otherwise return some err
//
//   // the parent goroutine will spawn N child go routines, the children will do the following:
//   // writing phase:
//   //   select a random <testdir>/in/{00..NN}
//   //   create and open for writing a new file in that directory
//   //   the file will be named `{HOSTNAME}.{PID}.{CHILDNO}.{UNIXTS+NANO}`
//   //      {CHILDNO} is just a number from 1 to N where N is the number of child goroutines
//   //   then, to this file, random data will be written in BlockSize chunks, until the file is FileSize total
//   //      the writes will be in O_DIRECT or O_SYNC if those arguments were set
//   //      if FsyncFreq is given, fsync() will sometimes be called after writes based on the frquency
//   //      if FsyncCloseFreq is given fsync() will be sometimes be called before the file is closed based on the frquency
//   //   we will continue creating, opening, and writing these files in this dir in a loop until we've created
//   //   FilesPerBatch number of them, at which point they will all be moved (ranamed) to <testdir>/out/NN where NN
//   //   is the same random numberd dir we selected earlier
//   //   one caveat is that if FilesPerMove is set, we may move files before we are done ceating in this dir
//   //   for example if FilesPerBatch is 10, and FilesPerMOve is 2,  every 2 files we will move them to out/NN, while
//   //   continuing to create them in the same dir until FilesPerBatch is reached, then we will move any files
//   //   not already moved and continue on.
//   // reading/unlinking phase:
//   //   the child goroutine then randomly selects a different NN dir, but this time in <testdir>/out/NN
//   //   it's important the NN isn't the same NN as was used in input/NN, in order to maximize the chances of
//   //   causing metadata capability conflicts
//   //   the goroutine then lists the files in this directory, and loops over each one, reading bytes from it in
//   //   BlockSize chunks until the whole file is read, then it closes and deletes the file.
//   //   once all files that were originally listed are read and closed, or MaxReadFiles have been processed;
//   //     go back to the writing phase in another random in/NN dir and repeat the whole process.
//   //     it's important that during the reading/deleteing phase, that the list dents is not run again. list the dents
//   //     once, process the files, move on.
//
//   // counters of each data and metadata operation should be kept, counters of the total bytes read and written should be kept
//   // min, max, avg latency for each operation should be kept.
//
//   return TestResult{}, nil
// }
//
