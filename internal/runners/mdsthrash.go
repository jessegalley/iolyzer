// mdsthrash.go contains functionality for the `mdsthrash` test
// this is an example implementation that generates fake operations
// to demonstrate the statistics collection system
package runners

import (
	"context"
	"math/rand"
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

// runMDSThrashWorker simulates a single worker performing mdsthrash operations
func runMDSThrashWorker(ctx context.Context, workerID int, config *config.Config, collector *stats.StatsCollector) {
	// determine whether to collect latency based on collector configuration
	// this could also be passed as a separate parameter if needed
	collectLatency := true // assume latency collection for now, could be derived from config

	// create worker statistics tracker
	tracker := stats.NewWorkerStatsTracker(workerID, collector, 500*time.Millisecond, collectLatency)
	defer tracker.Finalize()

	// create worker-specific random number generator for consistent behavior
	rng := rand.New(rand.NewSource(time.Now().UnixNano() + int64(workerID)))

	// track current batch state
	filesInCurrentBatch := 0
	totalFilesCreated := 0

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
		case filesInCurrentBatch < config.FilesPerBatch:
			// phase 1: create and write files
			performCreateAndWrite(tracker, rng, config, collectLatency)
			filesInCurrentBatch++
			totalFilesCreated++

		case config.FilesPerMove > 0 && totalFilesCreated%config.FilesPerMove == 0:
			// phase 2: move files to output directory
			performMoveOperation(tracker, rng, config, collectLatency)
			filesInCurrentBatch = 0 // reset batch counter after move

		default:
			// phase 3: read and unlink operations
			if config.MaxReadFiles == 0 || totalFilesCreated < config.MaxReadFiles {
				performReadAndUnlink(tracker, rng, config, collectLatency)
			}
			filesInCurrentBatch = 0 // reset batch counter
		}

		// small delay to prevent busy loop and make display updates visible
		time.Sleep(time.Millisecond)
	}
}

// performCreateAndWrite simulates creating a file and writing data to it
func performCreateAndWrite(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
	// simulate file creation operation
	// TODO: replace with actual file creation (os.Create, etc.)
	createLatency := simulateLatency(rng, 50, 200) // 50-200 microseconds
	if collectLatency {
		tracker.RecordOperation("create", createLatency)
	} else {
		tracker.RecordOperationCount("create")
	}

	// simulate writing data to the file
	// TODO: replace with actual write operations (file.Write, etc.)
	writeLatency := simulateLatency(rng, 100, 500) // 100-500 microseconds
	if collectLatency {
		tracker.RecordOperation("write", writeLatency)
	} else {
		tracker.RecordOperationCount("write")
	}

	// simulate fsync operation based on frequency setting
	if config.FsyncCloseFreq > 0 && rng.Intn(100) < config.FsyncCloseFreq {
		// TODO: replace with actual fsync operation (file.Sync())
		fsyncLatency := simulateLatency(rng, 500, 2000) // 500-2000 microseconds
		if collectLatency {
			tracker.RecordOperation("fsync", fsyncLatency)
		} else {
			tracker.RecordOperationCount("fsync")
		}
	}
}

// performMoveOperation simulates moving files from input to output directory
func performMoveOperation(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
	// simulate move/rename operation
	// TODO: replace with actual move operation (os.Rename, etc.)
	moveLatency := simulateLatency(rng, 200, 800) // 200-800 microseconds
	if collectLatency {
		tracker.RecordOperation("move", moveLatency)
	} else {
		tracker.RecordOperationCount("move")
	}
}

// performReadAndUnlink simulates reading file content and then unlinking it
func performReadAndUnlink(tracker *stats.WorkerStatsTracker, rng *rand.Rand, config *config.Config, collectLatency bool) {
	// simulate reading file content
	// TODO: replace with actual read operation (file.Read, ioutil.ReadFile, etc.)
	readLatency := simulateLatency(rng, 80, 300) // 80-300 microseconds
	if collectLatency {
		tracker.RecordOperation("read", readLatency)
	} else {
		tracker.RecordOperationCount("read")
	}

	// simulate unlinking the file
	// TODO: replace with actual unlink operation (os.Remove, etc.)
	unlinkLatency := simulateLatency(rng, 30, 150) // 30-150 microseconds
	if collectLatency {
		tracker.RecordOperation("unlink", unlinkLatency)
	} else {
		tracker.RecordOperationCount("unlink")
	}
}

// simulateLatency generates a realistic random latency within the specified range
// TODO: remove this function when replacing with actual I/O operations
func simulateLatency(rng *rand.Rand, minMicros, maxMicros int) time.Duration {
	// generate random latency in microseconds
	latencyMicros := minMicros + rng.Intn(maxMicros-minMicros)

	// add some occasional spikes to simulate real filesystem behavior
	if rng.Intn(100) < 5 { // 5% chance of spike
		latencyMicros *= 2 + rng.Intn(3) // 2-4x longer
	}

	return time.Duration(latencyMicros) * time.Microsecond
}
