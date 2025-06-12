// mdsthrash.go contains functionality for the `mdsthrash` test
// this test performs intensive metadata operations to stress filesystem metadata servers
package runners

import (
	"context"
	"crypto/rand"
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
type workerState struct {
	workerID          int      // unique identifier for this worker
	inputDirIndex     int      // current input directory index (1-based)
	outputDirIndex    int      // current output directory index (1-based)
	filesInBatch      int      // number of files created in current batch
	totalFilesCreated int      // total files created by this worker
	createdFiles      []string // list of files created that can be moved/read
	movedFiles        []string // list of files moved to output directories
}

// RunMDSThrash executes the mdsthrash test with the provided configuration and collector
func RunMDSThrash(config *config.Config, collector *stats.StatsCollector) (MDSThrashResult, error) {
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

	// get final statistics from collector
	finalStats := collector.GetFinalStats()

	// create result summary for compatibility/logging
	result := MDSThrashResult{
		TestDuration: time.Duration(finalStats.TestDuration * float64(time.Second)),
		CreateOps:    finalStats.TotalCounts["create"],
		WriteOps:     finalStats.TotalCounts["write"],
		MoveOps:      finalStats.TotalCounts["move"],
		ReadOps:      finalStats.TotalCounts["read"],
		UnlinkOps:    finalStats.TotalCounts["unlink"],
		FsyncOps:     finalStats.TotalCounts["fsync"],
	}

	// calculate total files processed (creates are new files)
	result.TotalFiles = result.CreateOps

	return result, nil
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
		workerID:          workerID,
		inputDirIndex:     1,
		outputDirIndex:    1,
		filesInBatch:      0,
		totalFilesCreated: 0,
		createdFiles:      make([]string, 0),
		movedFiles:        make([]string, 0),
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

		// simulate the mdsthrash workflow phases
		switch {
		case state.filesInBatch < config.FilesPerBatch:
			// phase 1: create and write files
			err := performCreateAndWrite(tracker, rng, config, state, collectLatency)
			if err != nil {
				// log error but continue with test
				fmt.Fprintf(os.Stderr, "worker %d create/write error: %v\n", workerID, err)
			} else {
				state.filesInBatch++
				state.totalFilesCreated++
			}

		case config.FilesPerMove > 0 && state.totalFilesCreated%config.FilesPerMove == 0:
			// phase 2: move files to output directory
			err := performMoveOperation(tracker, rng, config, state, collectLatency)
			if err != nil {
				// log error but continue with test
				fmt.Fprintf(os.Stderr, "worker %d move error: %v\n", workerID, err)
			} else {
				state.filesInBatch = 0 // reset batch counter after move
			}

		default:
			// phase 3: read and unlink operations
			if config.MaxReadFiles == 0 || state.totalFilesCreated < config.MaxReadFiles {
				err := performReadAndUnlink(tracker, rng, config, state, collectLatency)
				if err != nil {
					// log error but continue with test
					fmt.Fprintf(os.Stderr, "worker %d read/unlink error: %v\n", workerID, err)
				}
			}
			state.filesInBatch = 0 // reset batch counter
		}

		// small delay to prevent busy loop and make display updates visible
		time.Sleep(time.Millisecond)
	}
}

// performCreateAndWrite creates a file in the input directory and writes data to it
func performCreateAndWrite(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// construct input directory path
	inputDir := filepath.Join(config.TestDir, "in", fmt.Sprintf("%02d", state.inputDirIndex))

	// generate unique filename using worker id and file counter
	fileName := fmt.Sprintf("file_%d_%d_%d", state.workerID, state.inputDirIndex, state.totalFilesCreated)
	filePath := filepath.Join(inputDir, fileName)

	// measure create operation latency
	createStart := time.Now()

	// create the file
	file, err := os.Create(filePath)
	if err != nil {
		return fmt.Errorf("failed to create file %s: %w", filePath, err)
	}

	// record create operation timing
	createLatency := time.Since(createStart)
	if collectLatency {
		tracker.RecordOperation("create", createLatency)
	} else {
		tracker.RecordOperationCount("create")
	}

	// add to created files list for potential move/read operations
	state.createdFiles = append(state.createdFiles, filePath)

	// generate random data for writing
	dataSize := config.FileSize
	if dataSize <= 0 {
		dataSize = 4096 // default to 4kb if not specified
	}

	writeData := make([]byte, dataSize)
	_, err = rand.Read(writeData)
	if err != nil {
		file.Close()
		return fmt.Errorf("failed to generate random data: %w", err)
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

	// advance to next input directory if we've filled current batch
	if state.filesInBatch >= config.FilesPerBatch-1 {
		state.inputDirIndex++
		if state.inputDirIndex > config.DirCount {
			state.inputDirIndex = 1 // wrap around to first directory
		}
	}

	return nil
}

// performMoveOperation moves a file from input directory to output directory
func performMoveOperation(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// check if we have files to move
	if len(state.createdFiles) == 0 {
		// no files available to move, skip this operation
		return nil
	}

	// select a random file from created files list
	fileIndex := rng.Intn(len(state.createdFiles))
	sourcePath := state.createdFiles[fileIndex]

	// remove from created files list
	state.createdFiles = append(state.createdFiles[:fileIndex], state.createdFiles[fileIndex+1:]...)

	// construct destination path in output directory
	sourceFileName := filepath.Base(sourcePath)
	outputDir := filepath.Join(config.TestDir, "out", fmt.Sprintf("%02d", state.outputDirIndex))
	destPath := filepath.Join(outputDir, sourceFileName)

	// measure move operation latency
	moveStart := time.Now()

	// perform the move operation using os.Rename
	err := os.Rename(sourcePath, destPath)
	if err != nil {
		// add file back to created files list if move failed
		state.createdFiles = append(state.createdFiles, sourcePath)
		return fmt.Errorf("failed to move file %s to %s: %w", sourcePath, destPath, err)
	}

	// record move operation timing
	moveLatency := time.Since(moveStart)
	if collectLatency {
		tracker.RecordOperation("move", moveLatency)
	} else {
		tracker.RecordOperationCount("move")
	}

	// add to moved files list for potential read operations
	state.movedFiles = append(state.movedFiles, destPath)

	// advance to next output directory
	state.outputDirIndex++
	if state.outputDirIndex > config.DirCount {
		state.outputDirIndex = 1 // wrap around to first directory
	}

	return nil
}

// performReadAndUnlink reads file content and then removes the file
func performReadAndUnlink(tracker *stats.WorkerStatsTracker, rng *mathrand.Rand, config *config.Config, state *workerState, collectLatency bool) error {
	// determine which file list to use for read operations
	var targetFiles []string
	var fileIndex int

	// prefer moved files, but fall back to created files if needed
	if len(state.movedFiles) > 0 {
		targetFiles = state.movedFiles
		fileIndex = rng.Intn(len(state.movedFiles))
	} else if len(state.createdFiles) > 0 {
		targetFiles = state.createdFiles
		fileIndex = rng.Intn(len(state.createdFiles))
	} else {
		// no files available for read/unlink, skip this operation
		return nil
	}

	filePath := targetFiles[fileIndex]

	// measure read operation latency
	readStart := time.Now()

	// open file for reading
	file, err := os.Open(filePath)
	if err != nil {
		// file might have been removed by another worker, skip silently
		return nil
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
		// file might have been removed by another worker, skip silently
		return nil
	}

	// record unlink operation timing
	unlinkLatency := time.Since(unlinkStart)
	if collectLatency {
		tracker.RecordOperation("unlink", unlinkLatency)
	} else {
		tracker.RecordOperationCount("unlink")
	}

	// remove file from appropriate tracking list
	if len(state.movedFiles) > 0 && fileIndex < len(state.movedFiles) {
		// remove from moved files list
		state.movedFiles = append(state.movedFiles[:fileIndex], state.movedFiles[fileIndex+1:]...)
	} else if len(state.createdFiles) > 0 && fileIndex < len(state.createdFiles) {
		// remove from created files list
		state.createdFiles = append(state.createdFiles[:fileIndex], state.createdFiles[fileIndex+1:]...)
	}

	return nil
}

// mdsthrash.go contains functionality for the `mdsthrash` test
// this is an example implementation that generates fake operations
// to demonstrate the statistics collection system
// package runners
//
// import (
// 	"context"
// 	"math/rand"
// 	"sync"
// 	"time"
//
// 	"github.com/jessegalley/iolyzer/internal/config"
// 	"github.com/jessegalley/iolyzer/internal/stats"
// )
//
// // MDSThrashResult contains the results from a completed mdsthrash test
// type MDSThrashResult struct {
// 	TestDuration time.Duration // actual test duration
// 	TotalFiles   int64         // total files processed
// 	CreateOps    int64         // number of create operations
// 	WriteOps     int64         // number of write operations
// 	MoveOps      int64         // number of move operations
// 	ReadOps      int64         // number of read operations
// 	UnlinkOps    int64         // number of unlink operations
// 	FsyncOps     int64         // number of fsync operations
// }
//
// // RunMDSThrash executes the mdsthrash test with the provided configuration and collector
// func RunMDSThrash(config *config.Config, collector *stats.StatsCollector) (MDSThrashResult, error) {
// 	// create context for coordinating worker shutdown
// 	ctx, cancel := context.WithTimeout(context.Background(), config.TestDuration)
// 	defer cancel()
//
// 	// create wait group for worker synchronization
// 	var wg sync.WaitGroup
//
// 	// launch worker goroutines
// 	for i := 0; i < config.ParallelJobs; i++ {
// 		wg.Add(1)
// 		go func(workerID int) {
// 			defer wg.Done()
// 			runMDSThrashWorker(ctx, workerID, config, collector)
// 		}(i)
// 	}
//
// 	// wait for all workers to complete
// 	wg.Wait()
//
// 	// get final statistics from collector
// 	finalStats := collector.GetFinalStats()
//
// 	// create result summary for compatibility/logging
// 	result := MDSThrashResult{
// 		TestDuration: time.Duration(finalStats.TestDuration * float64(time.Second)),
// 		CreateOps:    finalStats.TotalCounts["create"],
// 		WriteOps:     finalStats.TotalCounts["write"],
// 		MoveOps:      finalStats.TotalCounts["move"],
// 		ReadOps:      finalStats.TotalCounts["read"],
// 		UnlinkOps:    finalStats.TotalCounts["unlink"],
// 		FsyncOps:     finalStats.TotalCounts["fsync"],
// 	}
//
// 	// calculate total files processed (creates are new files)
// 	result.TotalFiles = result.CreateOps
//
// 	return result, nil
// }
//
// // runMDSThrashWorker simulates a single worker performing mdsthrash operations
// func runMDSThrashWorker(ctx context.Context, workerID int, config *config.Config, collector *stats.StatsCollector) {
// 	// determine whether to collect latency based on collector configuration
// 	// this could also be passed as a separate parameter if needed
// 	collectLatency := true// assume latency collection for now, could be derived from config
//
// 	// create worker statistics tracker
// 	tracker := stats.NewWorkerStatsTracker(workerID, collector, 500*time.Millisecond, collectLatency)
// 	defer tracker.Finalize()
//
// 	// create worker-specific random number generator for consistent behavior
// 	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))
//
// 	// track current batch state
// 	filesInCurrentBatch := 0
// 	totalFilesCreated := 0
//
// 	// main worker loop - continue until context is cancelled
// 	for {
// 		select {
// 		case <-ctx.Done():
// 			// test duration exceeded or context cancelled
// 			return
// 		default:
// 			// continue with operations
// 		}
//
// 		// simulate the mdsthrash workflow phases
// 		switch {
// 		case filesInCurrentBatch < config.FilesPerBatch:
// 			// phase 1: create and write files
// 			performCreateAndWrite(tracker, rng, config, collectLatency)
// 			filesInCurrentBatch++
// 			totalFilesCreated++
//
// 		case config.FilesPerMove > 0 && totalFilesCreated%config.FilesPerMove == 0:
// 			// phase 2: move files to output directory
// 			performMoveOperation(tracker, rng, config, collectLatency)
// 			filesInCurrentBatch = 0 // reset batch counter after move
//
// 		default:
// 			// phase 3: read and unlink operations
// 			if config.MaxReadFiles == 0 || totalFilesCreated < config.MaxReadFiles {
// 				performReadAndUnlink(tracker, rng, config, collectLatency)
// 			}
// 			filesInCurrentBatch = 0 // reset batch counter
// 		}
//
// 		// small delay to prevent busy loop and make display updates visible
// 		// time.Sleep(time.Millisecond)
// 	}
// }
//
// // performCreateAndWrite simulates creating a file and writing data to it
// func performCreateAndWrite(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
// 	// simulate file creation operation
// 	// TODO: replace with actual file creation (os.Create, etc.)
// 	createLatency := simulateLatency(rng, 50, 200) // 50-200 microseconds
//   time.Sleep(createLatency)
// 	if collectLatency {
// 		tracker.RecordOperation("create", createLatency)
// 	} else {
// 		tracker.RecordOperationCount("create")
// 	}
//
// 	// simulate writing data to the file
// 	// TODO: replace with actual write operations (file.Write, etc.)
// 	writeLatency := simulateLatency(rng, 100, 500) // 100-500 microseconds
//   time.Sleep(writeLatency)
// 	if collectLatency {
// 		tracker.RecordOperation("write", writeLatency)
// 	} else {
// 		tracker.RecordOperationCount("write")
// 	}
//
// 	// simulate fsync operation based on frequency setting
// 	if config.FsyncCloseFreq > 0 && rng.Intn(100) < config.FsyncCloseFreq {
// 		// TODO: replace with actual fsync operation (file.Sync())
// 		fsyncLatency := simulateLatency(rng, 500, 2000) // 500-2000 microseconds
//     time.Sleep(fsyncLatency)
// 		if collectLatency {
// 			tracker.RecordOperation("fsync", fsyncLatency)
// 		} else {
// 			tracker.RecordOperationCount("fsync")
// 		}
// 	}
// }
//
// // performMoveOperation simulates moving files from input to output directory
// func performMoveOperation(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
// 	// simulate move/rename operation
// 	// TODO: replace with actual move operation (os.Rename, etc.)
// 	moveLatency := simulateLatency(rng, 200, 800) // 200-800 microseconds
//   time.Sleep(moveLatency)
// 	if collectLatency {
// 		tracker.RecordOperation("move", moveLatency)
// 	} else {
// 		tracker.RecordOperationCount("move")
// 	}
// }
//
// // performReadAndUnlink simulates reading file content and then unlinking it
// func performReadAndUnlink(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
// 	// simulate reading file content
// 	// TODO: replace with actual read operation (file.Read, ioutil.ReadFile, etc.)
// 	readLatency := simulateLatency(rng, 80, 300) // 80-300 microseconds
//   time.Sleep(readLatency)
// 	if collectLatency {
// 		tracker.RecordOperation("read", readLatency)
// 	} else {
// 		tracker.RecordOperationCount("read")
// 	}
//
// 	// simulate unlinking the file
// 	// TODO: replace with actual unlink operation (os.Remove, etc.)
// 	unlinkLatency := simulateLatency(rng, 30, 150) // 30-150 microseconds
//   time.Sleep(unlinkLatency)
// 	if collectLatency {
// 		tracker.RecordOperation("unlink", unlinkLatency)
// 	} else {
// 		tracker.RecordOperationCount("unlink")
// 	}
// }
//
// // simulateLatency generates a realistic random latency within the specified range
// // TODO: remove this function when replacing with actual I/O operations
// func simulateLatency(rng *rand.Rand, minMicros, maxMicros int) time.Duration {
// 	// generate random latency in microseconds
// 	latencyMicros := minMicros + rng.Intn(maxMicros-minMicros)
//
// 	// add some occasional spikes to simulate real filesystem behavior
// 	if rng.Intn(100) < 5 { // 5% chance of spike
// 		latencyMicros *= 2 + rng.Intn(3) // 2-4x longer
// 	}
//
// 	return time.Duration(latencyMicros) * time.Microsecond
// }
