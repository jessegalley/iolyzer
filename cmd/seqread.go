/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jessegalley/iolyzer/internal/layout"
	"github.com/jessegalley/iolyzer/internal/output"
	"github.com/jessegalley/iolyzer/internal/runners"
	ioworker "github.com/jessegalley/iolyzer/internal/workers"
	"github.com/spf13/cobra"
)

// seqreadCmd represents the seqread command
var seqreadCmd = &cobra.Command{
	Use:   "seqread [test_path]",
	Short: "Sequential read-only",
	Long: `Lays out test files for each parallel worker and performs a pure, sequential read test.`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// if positional arg was given, override the 
		// default test dir set in root 
		if len(args) == 1 {
			testDir = args[0]
		} 

		// validate that the testdir (default or arg) exists 
		// and is writable by the calling user. create it 
		// if possible
		if err := ensureWritableDirectory(testDir); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
			os.Exit(1)
		}

		// execute this test 
		if err := runSeqRead(testDir); err != nil {
			fmt.Fprintf(os.Stderr, "test failed: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(seqreadCmd)
}

// runSeqRead executes the sequential read test
func runSeqRead(testDir string) error {
	// create slice to track test files
	var testFiles []string

	// validate output format
	format, err := output.ValidateFormat(outFmt)
	if err != nil {
		return fmt.Errorf("invalid output format: %w", err)
	}

	// create test files for each worker
	for i := 0; i < parallelJobs; i++ {
		// generate unique file name for this worker
		workerFile := fmt.Sprintf("%s_%d.dat", fileName, i)

		// create the full path
		fullPath := filepath.Join(testDir, workerFile)

		// ensure path is absolute
		absPath, err := filepath.Abs(fullPath)
		if err != nil {
			return fmt.Errorf("failed to get absolute path: %w", err)
		}

		// create the test file
		if err := layout.LayoutTestFile(absPath, int(fileSize*1024*1024), reinitFile); err != nil {
			return fmt.Errorf("failed to create test file %s: %w", workerFile, err)
		}

		// track the file
		testFiles = append(testFiles, absPath)
	}

	// announce test start
	fmt.Fprintf(os.Stderr, "starting sequential read test with %d workers\n", parallelJobs)

	// create channels for worker communication
	opChan := make(chan ioworker.OP, parallelJobs)
	resultsChan := make(chan map[string]uint64, parallelJobs)
	errorsChan := make(chan error, parallelJobs)
  // channel for signaling producer to stop
  producerDone := make(chan struct{})

  // start producer before everything else
  go func() {
    for {
      select {
      case <-producerDone:
        close(opChan)
        return
      default:
        opChan <- ioworker.OP{
          OpType:   "read",
          OpRepeat: 8,
        }
      }
    }
  }()

	// create wait group for worker synchronization
	var wg sync.WaitGroup

	// track aggregate metrics
	aggMetrics := struct {
		sync.Mutex
		readCount   int64
		bytesRead   int64
		readErrors  int64
		elapsedTime time.Duration
	}{}

	// create slice to track workers for cleanup
	workers := make([]*ioworker.IOWorker, parallelJobs)

	// launch workers
	for i := 0; i < parallelJobs; i++ {
		// create new worker
		worker := ioworker.NewIOWorker(
			i,
			testDir,
			blockSize,
			directIO,
			oSync,
			fsyncFreq,
		)

		// store worker for cleanup
		workers[i] = worker

		// set channels
		worker.OpQueue = opChan
		worker.Results = resultsChan
		worker.ErrorsCh = errorsChan

		// start the worker
		if err := worker.Start(); err != nil {
			return fmt.Errorf("failed to start worker %d: %w", i, err)
		}

		// increment wait group
		wg.Add(1)

		// launch goroutine to handle worker results
		go func() {
			defer wg.Done()
			for result := range resultsChan {
        // fmt.Printf("DEBUG: reading results chan...\n")
				aggMetrics.Lock()
				aggMetrics.readCount += int64(result["reads"])
				aggMetrics.bytesRead += int64(result["bytes_read"])
				aggMetrics.Unlock()
			}
		}()
	}

	// start timer
	// startTime := time.Now()
  var stopTime time.Duration

	// // producer goroutine - generate read operations
	// go func() {
	// 	// keep generating ops until test duration is reached
	// 	for time.Since(startTime) < time.Duration(testDuration)*time.Second {
	// 		// send read operation to workers
	// 		opChan <- ioworker.OP{
	// 			OpType:   "read",
	// 			OpRepeat: 64,
	// 		}
	// 	}
	//
 //    stopTime = time.Since(startTime)
	// 	// close op channel when done
	// 	close(opChan)
	// }()

	// error handling goroutine
	go func() {
		for err := range errorsChan {
			aggMetrics.Lock()
			aggMetrics.readErrors++
			aggMetrics.Unlock()
			fmt.Fprintf(os.Stderr, "worker error: %v\n", err)
		}
	}()

	// wait for test duration
  fmt.Printf("DEBUG: parent sleeping...\n")
	time.Sleep(time.Duration(testDuration) * time.Second)
  close(producerDone)
  fmt.Printf("DEBUG: parent awake...\n")

	// stop all workers
  // var stopTime time.Duration
	for _, worker := range workers {
    fmt.Printf("DEBUG: calling stop...\n")
		worker.Stop()
    // stopTime = time.Since(startTime)
	}

	// wait for result processing to complete
  fmt.Printf("DEBUG: wg.Wait()\n")
  close(resultsChan)
	wg.Wait()
  fmt.Printf("DEBUG: after wg.Wait()\n")

  fmt.Printf("DEBUG: stopTime: %v\n", stopTime)
  fmt.Printf("DEBUG: testDuration: %v\n", testDuration)
  fmt.Printf("DEBUG: parsedDur: %v\n", time.Duration(testDuration))
	// record elapsed time
	// aggMetrics.elapsedTime = time.Since(startTime)
	aggMetrics.elapsedTime = time.Duration(testDuration)*time.Second

	// create final result
	result := runners.TestResult{
		ReadCount:  aggMetrics.readCount,
		WriteCount: 0, // sequential read test has no writes
		BytesRead:  aggMetrics.bytesRead,
		BytesWritten: 0,
		Duration:   aggMetrics.elapsedTime,
	}

	// format and output results
	output, err := output.FormatResult(result, format)
	if err != nil {
		return fmt.Errorf("failed to format results: %w", err)
	}

	// print results
	fmt.Print(output)

	return nil
}
