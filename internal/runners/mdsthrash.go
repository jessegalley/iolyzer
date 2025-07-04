// mdsthrash.go contains functionality for the `mdsthrash` test
// this test performs intensive metadata operations to stress filesystem metadata servers
package runners

import (
	"context"
	// "crypto/rand"
	"fmt"
	"io"
	mathrand "math/rand"
	"os"
	"path/filepath"
	"sync"
	"time"

	"github.com/jessegalley/iolyzer/internal/config"
	"github.com/jessegalley/iolyzer/internal/stats"
)

// MDSThrashResult contains the results from a completed mdsthrash test
type MDSThrashResult struct {
	TestDuration time.Duration // actual test duration
	TotalFiles   int64         // total files processed
	CreateOps    int64         // number of create operations
	WriteOps     int64         // number of write operations
	MoveOps      int64         // number of move operations
	ReadOps      int64         // number of read operations
	UnlinkOps    int64         // number of unlink operations
	FsyncOps     int64         // number of fsync operations
}

// workerState tracks the current state of a single worker thread
// simplified to only track essential batch state
type workerState struct {
	workerID     int      // unique identifier for this worker
	filesInBatch int      // number of files created in current batch
	recentFiles  []string // files created in current batch (for move operations)
}

// RunMDSThrash executes the mdsthrash test with the provided configuration and collector
func RunMDSThrash(config *config.Config, collector *stats.StatsCollector) error {
	// create context for coordinating worker shutdown
	ctx, cancel := context.WithTimeout(context.Background(), config.TestDuration)
	defer cancel()

	// create wait group for worker synchronization
	var wg sync.WaitGroup

	// launch worker goroutines
	for i := 0; i < config.ParallelJobs; i++ {
		wg.Add(1)
		go func(workerID int) {
			defer wg.Done()
			runMDSThrashWorker(ctx, workerID, config, collector)
		}(i)
	}

	// wait for all workers to complete
	wg.Wait()

	//
	// don't need to call the collector final stats here, it can
	// be called above this in the iotest package where the
	// collector and display threads are created
	// calling it here can potentially cause a race condition anyway
	//

	// get final statistics from collector
	// finalStats := collector.GetFinalStats()

	// // create result summary for compatibility/logging
	// result := MDSThrashResult{
	// 	TestDuration: time.Duration(finalStats.TestDuration * float64(time.Second)),
	// 	CreateOps:    finalStats.TotalCounts["create"],
	// 	WriteOps:     finalStats.TotalCounts["write"],
	// 	MoveOps:      finalStats.TotalCounts["move"],
	// 	ReadOps:      finalStats.TotalCounts["read"],
	// 	UnlinkOps:    finalStats.TotalCounts["unlink"],
	// 	FsyncOps:     finalStats.TotalCounts["fsync"],
	// }
	//
	// // calculate total files processed (creates are new files)
	// result.TotalFiles = result.CreateOps

	// return result, nil
	return nil
}

// runMDSThrashWorker performs metadata operations for a single worker thread
func runMDSThrashWorker(ctx context.Context, workerID int, config *config.Config, collector *stats.StatsCollector) {
	// determine whether to collect latency based on collector configuration
	collectLatency := true

	// create worker statistics tracker
	tracker := stats.NewWorkerStatsTracker(workerID, collector, 500*time.Millisecond, collectLatency)
	defer tracker.Finalize()

	// create worker-specific random number generator for consistent behavior
	rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(workerID)))

	// initialize worker state
	state := &workerState{
		workerID:     workerID,
		filesInBatch: 0,
		recentFiles:  make([]string, 0),
	}

	// main worker loop - continue until context is cancelled
	for {
		select {
		case <-ctx.Done():
			// test duration exceeded or context cancelled
			return
		default:
			// continue with operations
		}

		// execute one complete batch cycle
		err := executeWorkflowCycle(ctx, tracker, rng, config, state, collectLatency)
		if err != nil {
			// log error but continue with test
			fmt.Fprintf(os.Stderr, "worker %d workflow error: %v\n", workerID, err)
		}

		// small delay to prevent busy loop and make display updates visible
		time.Sleep(time.Millisecond)
	}
}

// executeWorkflowCycle performs one complete mdsthrash workflow cycle
// this includes create/write batch, moves, and read/unlink operations
func executeWorkflowCycle(ctx context.Context, tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// phase 1: create/write batch with optional periodic moves
	err := executeCreateWriteBatch(ctx, tracker, rng, config, state, collectLatency)
	if err != nil {
		return fmt.Errorf("create/write batch failed: %w", err)
	}

	// phase 2: move any remaining files (if FilesPerMove == 0)
	if config.FilesPerMove == 0 && len(state.recentFiles) > 0 {
		err := executeBatchMove(tracker, rng, config, state, collectLatency)
		if err != nil {
			return fmt.Errorf("batch move failed: %w", err)
		}
	}

	// phase 3: read/unlink operations on discovered files
	err = executeReadUnlinkPhase(tracker, rng, config, collectLatency)
	if err != nil {
		return fmt.Errorf("read/unlink phase failed: %w", err)
	}

	// reset batch state for next cycle
	state.filesInBatch = 0
	state.recentFiles = state.recentFiles[:0] // clear slice but keep capacity

	return nil
}

// executeCreateWriteBatch creates a batch of files with optional periodic moves
func executeCreateWriteBatch(ctx context.Context, tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// randomly select directory index for this batch (1-based)
	dirIndex := rng.Intn(config.DirCount) + 1

	// create files until batch is complete
	for state.filesInBatch < config.FilesPerBatch {
		// check if context is cancelled during batch
		select {
		case <-ctx.Done():
			return ctx.Err()
		default:
		}

		// create and write a single file
		err := performCreateAndWrite(tracker, rng, config, state, dirIndex, collectLatency)
		if err != nil {
			return fmt.Errorf("failed to create/write file: %w", err)
		}

		state.filesInBatch++

		// check if we should perform periodic moves during batch
		if config.FilesPerMove > 0 && state.filesInBatch%config.FilesPerMove == 0 {
			// move the last FilesPerMove files created
			err := executePeriodicMove(tracker, rng, config, state, dirIndex, collectLatency)
			if err != nil {
				return fmt.Errorf("periodic move failed: %w", err)
			}
		}
	}

	return nil
}

// performCreateAndWrite creates a file in the specified input directory and writes data to it
func performCreateAndWrite(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, dirIndex int, collectLatency bool) error {
	// construct input directory path using the provided directory index
	inputDir := filepath.Join(config.TestDir, "in", fmt.Sprintf("%02d", dirIndex))

	// generate unique filename using worker id, directory, and current time
	fileName := fmt.Sprintf("file_%d_%d_%d", state.workerID, dirIndex, time.Now().UnixNano())
	filePath := filepath.Join(inputDir, fileName)

	// measure create operation latency
	createStart := time.Now()

	// create the file
	file, err := os.Create(filePath)
	createLatency := time.Since(createStart)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}

	// record create operation timing
	if collectLatency {
		tracker.RecordOperation("create", createLatency)
	} else {
		tracker.RecordOperationCount("create")
	}

	// add to recent files list for potential move operations
	state.recentFiles = append(state.recentFiles, filePath)

	// generate random data for writing
	dataSize := config.FileSize
	if dataSize <= 0 {
		dataSize = 4096 // default to 4kb if not specified
	}

	writeData := make([]byte, dataSize)
	// pprof shows that just slamming crypto rand here is a cpu hog
	// _, err = rand.Read(writeData)
	//  if err != nil {
	// 	file.Close()
	// 	return fmt.Errorf("failed to generate random data: %w", err)
	// }

	// if i so some simple little pattern of bytes instead it should
	// be a bit easier on the cpus
	pattern := byte(state.workerID % 256)
	for i := range writeData {
		writeData[i] = pattern
	}

	// measure write operation latency
	writeStart := time.Now()

	// write data to the file
	_, err = file.Write(writeData)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to write data to file %s: %w", filePath, err)
	}

	// record write operation timing
	writeLatency := time.Since(writeStart)
	if collectLatency {
		tracker.RecordOperation("write", writeLatency)
	} else {
		tracker.RecordOperationCount("write")
	}

	// handle fsync operation based on frequency setting
	if config.FsyncCloseFreq > 0 && rng.Intn(100) < config.FsyncCloseFreq {
		// measure fsync operation latency
		fsyncStart := time.Now()

		// perform fsync operation
		err = file.Sync()
		if err != nil {
			file.Close()
			return fmt.Errorf("failed to fsync file %s: %w", filePath, err)
		}

		// record fsync operation timing
		fsyncLatency := time.Since(fsyncStart)
		if collectLatency {
			tracker.RecordOperation("fsync", fsyncLatency)
		} else {
			tracker.RecordOperationCount("fsync")
		}
	}

	// close the file
	err = file.Close()
	if err != nil {
		return fmt.Errorf("failed to close file %s: %w", filePath, err)
	}

	return nil
}

// executePeriodicMove moves the most recently created files during batch processing
func executePeriodicMove(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, dirIndex int, collectLatency bool) error {
	// determine how many files to move
	filesToMove := config.FilesPerMove
	if filesToMove > len(state.recentFiles) {
		filesToMove = len(state.recentFiles)
	}

	if filesToMove == 0 {
		return nil // nothing to move
	}

	// move the last N files created (most recent files)
	startIndex := len(state.recentFiles) - filesToMove
	filesToMoveList := state.recentFiles[startIndex:]

	// perform moves to the same directory index
	for _, filePath := range filesToMoveList {
		err := performMoveOperation(tracker, config, filePath, dirIndex, collectLatency)
		if err != nil {
			return fmt.Errorf("failed to move file %s: %w", filePath, err)
		}
	}

	// remove moved files from recent files list
	state.recentFiles = state.recentFiles[:startIndex]

	return nil
}

// executeBatchMove moves all remaining files at the end of a batch (when FilesPerMove == 0)
func executeBatchMove(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// randomly select output directory for this batch move
	dirIndex := rng.Intn(config.DirCount) + 1

	// move all files in recentFiles list
	for _, filePath := range state.recentFiles {
		err := performMoveOperation(tracker, config, filePath, dirIndex, collectLatency)
		if err != nil {
			return fmt.Errorf("failed to move file %s: %w", filePath, err)
		}
	}

	// clear the recent files list
	state.recentFiles = state.recentFiles[:0]

	return nil
}

// performMoveOperation moves a file from input directory to the specified output directory
func performMoveOperation(tracker *stats.WorkerStatsTracker, config *config.Config, sourcePath string, dirIndex int, collectLatency bool) error {
	// construct destination path in output directory using specified index
	sourceFileName := filepath.Base(sourcePath)
	outputDir := filepath.Join(config.TestDir, "out", fmt.Sprintf("%02d", dirIndex))
	destPath := filepath.Join(outputDir, sourceFileName)

	// measure move operation latency
	moveStart := time.Now()

	// perform the move operation using os.Rename
	err := os.Rename(sourcePath, destPath)
	if err != nil {
		return fmt.Errorf("failed to move file %s to %s: %w", sourcePath, destPath, err)
	}

	// record move operation timing
	moveLatency := time.Since(moveStart)
	if collectLatency {
		tracker.RecordOperation("move", moveLatency)
	} else {
		tracker.RecordOperationCount("move")
	}

	return nil
}

// executeReadUnlinkPhase discovers and processes files in a randomly selected output directory
func executeReadUnlinkPhase(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, collectLatency bool) error {
	// randomly select an output directory to process
	dirIndex := rng.Intn(config.DirCount) + 1
	outputDir := filepath.Join(config.TestDir, "out", fmt.Sprintf("%02d", dirIndex))

	// discover files in the selected directory (single directory listing)
	dentsStart := time.Now()
	entries, err := os.ReadDir(outputDir)
	dentsLatency := time.Since(dentsStart)
	if err != nil {
		// directory might not exist or be accessible, skip silently
		return nil
	}

	if collectLatency {
		tracker.RecordOperation("getdents", dentsLatency)
	} else {
		tracker.RecordOperationCount("getdents")
	}

	// process all discovered files (limit by MaxReadFiles if specified)
	filesProcessed := 0
	for _, entry := range entries {
		// check MaxReadFiles limit
		if config.MaxReadFiles > 0 && filesProcessed >= config.MaxReadFiles {
			break
		}

		// skip directories, only process regular files
		if entry.IsDir() {
			continue
		}

		// construct full file path
		filePath := filepath.Join(outputDir, entry.Name())

		// perform read and unlink operations on this file
		err := performReadAndUnlink(tracker, filePath, collectLatency)
		if err != nil {
			// file might have been removed by another worker, continue with next file
			continue
		}

		filesProcessed++
	}

	return nil
}

// performReadAndUnlink reads file content and then removes the file
func performReadAndUnlink(tracker *stats.WorkerStatsTracker, filePath string, collectLatency bool) error {
	// measure read operation latency
	readStart := time.Now()

	// open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		// file might have been removed by another worker, return error to skip
		return fmt.Errorf("failed to open file %s: %w", filePath, err)
	}

	// read all file content
	_, err = io.ReadAll(file)
	file.Close()
	if err != nil {
		return fmt.Errorf("failed to read file %s: %w", filePath, err)
	}

	// record read operation timing
	readLatency := time.Since(readStart)
	if collectLatency {
		tracker.RecordOperation("read", readLatency)
	} else {
		tracker.RecordOperationCount("read")
	}

	// measure unlink operation latency
	unlinkStart := time.Now()

	// remove the file
	err = os.Remove(filePath)
	if err != nil {
		// file might have been removed by another worker, return error to skip
		return fmt.Errorf("failed to remove file %s: %w", filePath, err)
	}

	// record unlink operation timing
	unlinkLatency := time.Since(unlinkStart)
	if collectLatency {
		tracker.RecordOperation("unlink", unlinkLatency)
	} else {
		tracker.RecordOperationCount("unlink")
	}

	return nil
}
