package iotest

import (
	"context"
	"math"
	"sync"
	"time"
)

// WorkerOpStats contains accumulated statistics for a single operation type from one worker
type WorkerOpStats struct {
	Count       int64 // total number of operations completed
	TotalTimeUs int64 // sum of all operation latencies in microseconds (0 if latency not collected)
	SumSquares  int64 // sum of squares of all operation latencies in microseconds (0 if latency not collected)
	MinUs       int64 // minimum latency observed in microseconds (0 if latency not collected)
	MaxUs       int64 // maximum latency observed in microseconds (0 if latency not collected)
}

// WorkerStatsUpdate contains all statistics from a single worker at a point in time
type WorkerStatsUpdate struct {
	WorkerID       int                      // unique identifier for the worker sending this update
	Timestamp      time.Time                // when this update was generated
	OpStats        map[string]WorkerOpStats // statistics by operation name (read, write, create, etc.)
	CollectLatency bool                     // whether latency data is valid in this update
}

// AggregatedStats contains the combined statistics from all workers
type AggregatedStats struct {
	TotalCounts    map[string]int64          // total operations by type across all workers
	IOPS           map[string]float64        // operations per second by type
	TestDuration   float64                   // total test duration in seconds
	HasLatencyData bool                      // whether latency statistics are available
	LatencyStats   map[string]LatencyMetrics // latency statistics by operation type (nil if HasLatencyData=false)
}

// LatencyMetrics contains statistical measures for operation latencies
type LatencyMetrics struct {
	Count    int64   // number of operations used to calculate these metrics
	MeanUs   float64 // arithmetic mean latency in microseconds
	StdDevUs float64 // standard deviation of latencies in microseconds
	MinUs    float64 // minimum latency observed in microseconds
	MaxUs    float64 // maximum latency observed in microseconds
}

// StatsCollector manages the collection and aggregation of statistics from multiple workers
type StatsCollector struct {
	updateChan      chan WorkerStatsUpdate    // channel for receiving worker updates
	liveUpdatesChan chan AggregatedStats      // channel for sending live aggregate updates
	ctx             context.Context           // context for coordinating shutdown
	cancel          context.CancelFunc        // function to cancel the collection context
	wg              sync.WaitGroup            // wait group for coordinating goroutine shutdown
	workerStats     map[int]WorkerStatsUpdate // current statistics from each worker
	testStartTime   time.Time                 // when the test began (for duration calculation)
	collectLatency  bool                      // whether this collector should process latency data
}

// NewStatsCollector creates a new statistics collector with the specified configuration
func NewStatsCollector(updateBufferSize, liveUpdatesBufferSize int, collectLatency bool) *StatsCollector {
	ctx, cancel := context.WithCancel(context.Background())
	return &StatsCollector{
		updateChan:      make(chan WorkerStatsUpdate, updateBufferSize),
		liveUpdatesChan: make(chan AggregatedStats, liveUpdatesBufferSize),
		ctx:             ctx,
		cancel:          cancel,
		workerStats:     make(map[int]WorkerStatsUpdate),
		testStartTime:   time.Now(),
		collectLatency:  collectLatency,
	}
}

// Start begins the statistics collection goroutine
func (sc *StatsCollector) Start() {
	sc.wg.Add(1)
	go sc.collectStats()
}

// Stop gracefully shuts down the statistics collector and waits for completion
func (sc *StatsCollector) Stop() {
	sc.cancel()
	close(sc.updateChan)
	sc.wg.Wait()
	close(sc.liveUpdatesChan)
}

// SendUpdate allows workers to send their current accumulated statistics
// this is non-blocking and will drop updates if the channel buffer is full
func (sc *StatsCollector) SendUpdate(update WorkerStatsUpdate) {
	select {
	case sc.updateChan <- update:
		// update sent successfully
	default:
		// channel buffer is full, drop this update
	}
}

// GetLiveUpdates returns the channel for receiving live aggregate statistics
// consumers can read from this channel to get real-time updates during test execution
func (sc *StatsCollector) GetLiveUpdates() <-chan AggregatedStats {
	return sc.liveUpdatesChan
}

// GetFinalStats returns the final aggregated statistics after collection has stopped
// this should only be called after Stop() has completed
func (sc *StatsCollector) GetFinalStats() AggregatedStats {
	return sc.calculateCurrentAggregate()
}

// collectStats is the main collection goroutine that processes worker updates
func (sc *StatsCollector) collectStats() {
	defer sc.wg.Done()

	for {
		select {
		case update, ok := <-sc.updateChan:
			if !ok {
				// channel closed, send final update and exit
				sc.sendLiveUpdate()
				return
			}
			sc.processWorkerUpdate(update)

		case <-sc.ctx.Done():
			// context cancelled, drain remaining updates and exit
			for update := range sc.updateChan {
				sc.processWorkerUpdate(update)
			}
			sc.sendLiveUpdate()
			return
		}
	}
}

// processWorkerUpdate incorporates a worker update and sends live aggregate update
func (sc *StatsCollector) processWorkerUpdate(update WorkerStatsUpdate) {
	// store the worker's current statistics (replaces any previous stats from this worker)
	sc.workerStats[update.WorkerID] = update

	// calculate and send live aggregate update
	sc.sendLiveUpdate()
}

// sendLiveUpdate calculates current aggregates and sends them on the live updates channel
func (sc *StatsCollector) sendLiveUpdate() {
	aggregate := sc.calculateCurrentAggregate()

	// send live update (non-blocking)
	select {
	case sc.liveUpdatesChan <- aggregate:
		// live update sent successfully
	default:
		// no one is listening or channel is full, drop the update
	}
}

// calculateCurrentAggregate computes aggregate statistics from all current worker stats
func (sc *StatsCollector) calculateCurrentAggregate() AggregatedStats {
	// collect all unique operation names
	operationNames := make(map[string]bool)
	for _, workerUpdate := range sc.workerStats {
		for opName := range workerUpdate.OpStats {
			operationNames[opName] = true
		}
	}

	// aggregate statistics by operation
	aggregatedOps := make(map[string]WorkerOpStats)
	for opName := range operationNames {
		var combinedStats WorkerOpStats
		for _, workerUpdate := range sc.workerStats {
			if opStats, exists := workerUpdate.OpStats[opName]; exists {
				combinedStats = combineOpStats(combinedStats, opStats)
			}
		}
		aggregatedOps[opName] = combinedStats
	}

	// calculate test duration
	testDuration := time.Since(sc.testStartTime).Seconds()

	// create aggregate result
	aggregate := AggregatedStats{
		TotalCounts:    make(map[string]int64),
		IOPS:           make(map[string]float64),
		TestDuration:   testDuration,
		HasLatencyData: sc.collectLatency,
	}

	// populate counts and IOPS
	for opName, opStats := range aggregatedOps {
		aggregate.TotalCounts[opName] = opStats.Count
		if testDuration > 0 {
			aggregate.IOPS[opName] = float64(opStats.Count) / testDuration
		}
	}

	// populate latency statistics if available
	if sc.collectLatency {
		aggregate.LatencyStats = make(map[string]LatencyMetrics)
		for opName, opStats := range aggregatedOps {
			aggregate.LatencyStats[opName] = calculateLatencyMetrics(opStats)
		}
	}

	return aggregate
}

// combineOpStats merges two WorkerOpStats into a single combined result
func combineOpStats(stats1, stats2 WorkerOpStats) WorkerOpStats {
	combined := WorkerOpStats{
		Count:       stats1.Count + stats2.Count,
		TotalTimeUs: stats1.TotalTimeUs + stats2.TotalTimeUs,
		SumSquares:  stats1.SumSquares + stats2.SumSquares,
	}

	// calculate combined min (handle zero values from uninitialized stats)
	if stats1.MinUs == 0 {
		combined.MinUs = stats2.MinUs
	} else if stats2.MinUs == 0 {
		combined.MinUs = stats1.MinUs
	} else {
		combined.MinUs = min(stats1.MinUs, stats2.MinUs)
	}

	// calculate combined max
	combined.MaxUs = max(stats1.MaxUs, stats2.MaxUs)

	return combined
}

// calculateLatencyMetrics computes statistical measures from accumulated latency statistics
func calculateLatencyMetrics(stats WorkerOpStats) LatencyMetrics {
	if stats.Count == 0 {
		return LatencyMetrics{}
	}

	// calculate basic statistics (already in microseconds)
	meanUs := float64(stats.TotalTimeUs) / float64(stats.Count)
	minUs := float64(stats.MinUs)
	maxUs := float64(stats.MaxUs)

	// calculate standard deviation using: σ = √[(Σ(x²) - (Σ(x))²/n) / n]
	var stdDevUs float64
	if stats.Count > 1 {
		variance := (float64(stats.SumSquares) - float64(stats.TotalTimeUs)*meanUs) / float64(stats.Count)
		if variance >= 0 { // protect against floating point precision issues
			stdDevUs = math.Sqrt(variance)
		}
	}

	return LatencyMetrics{
		Count:    stats.Count,
		MeanUs:   meanUs,
		StdDevUs: stdDevUs,
		MinUs:    minUs,
		MaxUs:    maxUs,
	}
}

// WorkerStatsTracker helps individual workers accumulate statistics locally
type WorkerStatsTracker struct {
	workerID       int                      // unique identifier for this worker
	collector      *StatsCollector          // reference to the stats collector
	opStats        map[string]WorkerOpStats // accumulated statistics by operation type
	lastUpdateTime time.Time                // when the last update was sent to collector
	updateInterval time.Duration            // how often to send updates to collector
	collectLatency bool                     // whether to collect latency data for this worker
}

// NewWorkerStatsTracker creates a new statistics tracker for a worker
func NewWorkerStatsTracker(workerID int, collector *StatsCollector, updateInterval time.Duration, collectLatency bool) *WorkerStatsTracker {
	return &WorkerStatsTracker{
		workerID:       workerID,
		collector:      collector,
		opStats:        make(map[string]WorkerOpStats),
		lastUpdateTime: time.Now(),
		updateInterval: updateInterval,
		collectLatency: collectLatency,
	}
}

// RecordOperation records the completion of an I/O operation with its latency
// latency parameter is ignored if collectLatency is false
func (wst *WorkerStatsTracker) RecordOperation(operation string, latency time.Duration) {
	// get or create stats for this operation
	stats := wst.opStats[operation]

	if wst.collectLatency {
		// collect full latency statistics
		latencyUs := latency.Microseconds()
		wst.updateOpStatsWithLatency(&stats, latencyUs)
	} else {
		// collect only operation count
		wst.updateOpStatsCountOnly(&stats)
	}

	// store updated stats
	wst.opStats[operation] = stats

	// check if it's time to send an update to the collector
	if time.Since(wst.lastUpdateTime) >= wst.updateInterval {
		wst.sendUpdate()
	}
}

// RecordOperationCount records the completion of an I/O operation without latency measurement
// this is a more efficient alternative when latency data is not needed
func (wst *WorkerStatsTracker) RecordOperationCount(operation string) {
	// get or create stats for this operation
	stats := wst.opStats[operation]
	wst.updateOpStatsCountOnly(&stats)
	wst.opStats[operation] = stats

	// check if it's time to send an update to the collector
	if time.Since(wst.lastUpdateTime) >= wst.updateInterval {
		wst.sendUpdate()
	}
}

// Finalize sends a final update to the collector (should be called when worker completes)
func (wst *WorkerStatsTracker) Finalize() {
	wst.sendUpdate()
}

// updateOpStatsWithLatency incorporates a new latency measurement into accumulated statistics
func (wst *WorkerStatsTracker) updateOpStatsWithLatency(stats *WorkerOpStats, latencyUs int64) {
	stats.Count++
	stats.TotalTimeUs += latencyUs
	stats.SumSquares += latencyUs * latencyUs

	// update min (handle first operation case)
	if stats.MinUs == 0 || latencyUs < stats.MinUs {
		stats.MinUs = latencyUs
	}

	// update max
	if latencyUs > stats.MaxUs {
		stats.MaxUs = latencyUs
	}
}

// updateOpStatsCountOnly increments only the operation count (no latency data)
func (wst *WorkerStatsTracker) updateOpStatsCountOnly(stats *WorkerOpStats) {
	stats.Count++
	// leave all latency fields as zero
}

// sendUpdate sends the current accumulated statistics to the collector
func (wst *WorkerStatsTracker) sendUpdate() {
	// create a copy of current stats to avoid race conditions
	statsCopy := make(map[string]WorkerOpStats)
	for opName, opStats := range wst.opStats {
		statsCopy[opName] = opStats
	}

	update := WorkerStatsUpdate{
		WorkerID:       wst.workerID,
		Timestamp:      time.Now(),
		OpStats:        statsCopy,
		CollectLatency: wst.collectLatency,
	}

	wst.collector.SendUpdate(update)
	wst.lastUpdateTime = time.Now()
}

// min returns the smaller of two int64 values
func min(a, b int64) int64 {
	if a < b {
		return a
	}
	return b
}

// max returns the larger of two int64 values
func max(a, b int64) int64 {
	if a > b {
		return a
	}
	return b
}
