package iotest

import (
	"context"
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"
)

// DisplayConfig contains configuration options for the statistics display
type DisplayConfig struct {
	UpdateInterval time.Duration // how often to refresh the display
	ShowLatency    bool          // whether to show latency statistics
	ShowProgress   bool          // whether to show a progress bar
	TestDuration   time.Duration // total test duration (for progress calculation)
	Quiet          bool          // suppress all live updates (final summary only)
}

// StatsDisplay manages real-time display of statistics during test execution
type StatsDisplay struct {
	config         DisplayConfig      // display configuration
	statsCollector *StatsCollector    // source of statistics updates
	ctx            context.Context    // context for coordinating shutdown
	cancel         context.CancelFunc // function to cancel the display context
	wg             sync.WaitGroup     // wait group for coordinating goroutine shutdown
	lastStats      AggregatedStats    // most recent statistics for comparison
	startTime      time.Time          // when the display started
}

// NewStatsDisplay creates a new statistics display with the specified configuration
func NewStatsDisplay(collector *StatsCollector, config DisplayConfig) *StatsDisplay {
	ctx, cancel := context.WithCancel(context.Background())
	return &StatsDisplay{
		config:         config,
		statsCollector: collector,
		ctx:            ctx,
		cancel:         cancel,
		startTime:      time.Now(),
	}
}

// Start begins the display goroutine that will show live statistics updates
func (sd *StatsDisplay) Start() {
	if sd.config.Quiet {
		// in quiet mode, don't start the display goroutine
		return
	}

	sd.wg.Add(1)
	go sd.displayLoop()
}

// Stop gracefully shuts down the statistics display and waits for completion
func (sd *StatsDisplay) Stop() {
	sd.cancel()
	sd.wg.Wait()
}

// ShowFinalSummary displays the final test results in a comprehensive format
func (sd *StatsDisplay) ShowFinalSummary(finalStats AggregatedStats) {
	// clear any live display artifacts
	if !sd.config.Quiet {
		fmt.Print("\033[2J\033[H") // clear screen and move cursor to top
	}

	fmt.Printf("\n=== Final Test Results ===\n\n")

	// show test duration
	fmt.Printf("Test Duration: %.2f seconds\n", finalStats.TestDuration)

	// get sorted list of operations for consistent output
	operations := sd.getSortedOperations(finalStats.TotalCounts)
	if len(operations) == 0 {
		fmt.Printf("No operations recorded\n")
		return
	}

	// display operation counts and IOPS
	fmt.Printf("\nOperation Summary:\n")
	fmt.Printf("%-12s %12s %12s\n", "Operation", "Count", "IOPS")
	fmt.Printf("%-12s %12s %12s\n", "─────────", "─────", "────")

	var totalOps int64
	var totalIOPS float64

	for _, op := range operations {
		count := finalStats.TotalCounts[op]
		iops := finalStats.IOPS[op]
		fmt.Printf("%-12s %12d %12.2f\n", op, count, iops)
		totalOps += count
		totalIOPS += iops
	}

	fmt.Printf("%-12s %12s %12s\n", "─────────", "─────", "────")
	fmt.Printf("%-12s %12d %12.2f\n", "Total", totalOps, totalIOPS)

	// display latency statistics if available
	if finalStats.HasLatencyData && sd.config.ShowLatency {
		fmt.Printf("\nLatency Statistics (microseconds):\n")
		fmt.Printf("%-12s %8s %8s %8s %8s\n", "Operation", "Mean", "StdDev", "Min", "Max")
		fmt.Printf("%-12s %8s %8s %8s %8s\n", "─────────", "────", "──────", "───", "───")

		for _, op := range operations {
			if latency, exists := finalStats.LatencyStats[op]; exists && latency.Count > 0 {
				fmt.Printf("%-12s %8.1f %8.1f %8.1f %8.1f\n",
					op, latency.MeanUs, latency.StdDevUs, latency.MinUs, latency.MaxUs)
			}
		}
	}

	fmt.Printf("\n")
}

// displayLoop is the main display goroutine that processes live statistics updates
func (sd *StatsDisplay) displayLoop() {
	defer sd.wg.Done()

	// create a ticker for regular display updates
	ticker := time.NewTicker(sd.config.UpdateInterval)
	defer ticker.Stop()

	// track if we've shown the header yet
	headerShown := false

	for {
		select {
		case stats, ok := <-sd.statsCollector.GetLiveUpdates():
			if !ok {
				// channel closed, display is done
				return
			}
			sd.processStatsUpdate(stats, &headerShown)

		case <-ticker.C:
			// periodic refresh even if no new stats (for progress bar updates)
			sd.refreshDisplay(&headerShown)

		case <-sd.ctx.Done():
			// context cancelled, display is done
			return
		}
	}
}

// processStatsUpdate handles a new statistics update from the collector
func (sd *StatsDisplay) processStatsUpdate(stats AggregatedStats, headerShown *bool) {
	sd.lastStats = stats
	sd.showLiveStats(stats, headerShown)
}

// refreshDisplay updates the display even without new statistics (for progress bar)
func (sd *StatsDisplay) refreshDisplay(headerShown *bool) {
	if sd.lastStats.TestDuration > 0 {
		sd.showLiveStats(sd.lastStats, headerShown)
	}
}

// showLiveStats displays current statistics in a live updating format
func (sd *StatsDisplay) showLiveStats(stats AggregatedStats, headerShown *bool) {
	// move cursor to top of screen for live updates
	if *headerShown {
		fmt.Print("\033[H") // move cursor to top
	}

	// show header if this is the first display
	if !*headerShown {
		fmt.Printf("=== Live Test Statistics ===\n\n")
		*headerShown = true
	}

	// show progress bar if enabled
	if sd.config.ShowProgress && sd.config.TestDuration > 0 {
		elapsed := time.Since(sd.startTime)
		progress := float64(elapsed) / float64(sd.config.TestDuration)
		if progress > 1.0 {
			progress = 1.0
		}
		sd.showProgressBar(progress, elapsed, sd.config.TestDuration)
		fmt.Printf("\n")
	}

	// show elapsed time
	fmt.Printf("Elapsed: %.1fs", stats.TestDuration)
	if sd.config.TestDuration > 0 {
		remaining := sd.config.TestDuration - time.Since(sd.startTime)
		if remaining > 0 {
			fmt.Printf(" | Remaining: %.1fs", remaining.Seconds())
		}
	}
	fmt.Printf("\n\n")

	// get sorted operations for consistent display
	operations := sd.getSortedOperations(stats.TotalCounts)
	if len(operations) == 0 {
		fmt.Printf("No operations recorded yet...\n")
		return
	}

	// display current IOPS
	fmt.Printf("%-12s %12s %12s", "Operation", "Count", "IOPS")
	if stats.HasLatencyData && sd.config.ShowLatency {
		fmt.Printf(" %10s", "Latency")
	}
	fmt.Printf("\n")

	fmt.Printf("%-12s %12s %12s", "─────────", "─────", "────")
	if stats.HasLatencyData && sd.config.ShowLatency {
		fmt.Printf(" %10s", "───────")
	}
	fmt.Printf("\n")

	for _, op := range operations {
		count := stats.TotalCounts[op]
		iops := stats.IOPS[op]
		fmt.Printf("%-12s %12d %12.2f", op, count, iops)

		// show average latency if available
		if stats.HasLatencyData && sd.config.ShowLatency {
			if latency, exists := stats.LatencyStats[op]; exists && latency.Count > 0 {
				fmt.Printf(" %8.1fμs", latency.MeanUs)
			} else {
				fmt.Printf(" %10s", "─")
			}
		}
		fmt.Printf("\n")
	}

	// clear any remaining lines from previous longer displays
	fmt.Printf("\033[J") // clear from cursor to end of screen
}

// showProgressBar displays a visual progress bar for the test
func (sd *StatsDisplay) showProgressBar(progress float64, elapsed, total time.Duration) {
	// progress bar configuration
	const barWidth = 40
	const progressChar = "█"
	const emptyChar = "░"

	// calculate filled portions
	filled := int(progress * float64(barWidth))
	if filled > barWidth {
		filled = barWidth
	}

	// build progress bar string
	bar := strings.Repeat(progressChar, filled) + strings.Repeat(emptyChar, barWidth-filled)

	// show progress bar with percentage and time info
	fmt.Printf("Progress: [%s] %.1f%% (%s / %s)",
		bar, progress*100, sd.formatDuration(elapsed), sd.formatDuration(total))
}

// formatDuration formats a duration for display in a human-readable format
func (sd *StatsDisplay) formatDuration(d time.Duration) string {
	if d < time.Minute {
		return fmt.Sprintf("%.0fs", d.Seconds())
	} else if d < time.Hour {
		return fmt.Sprintf("%.0fm%.0fs", d.Minutes(), d.Seconds()-60*d.Minutes())
	} else {
		hours := d.Hours()
		minutes := d.Minutes() - 60*hours
		return fmt.Sprintf("%.0fh%.0fm", hours, minutes)
	}
}

// getSortedOperations returns operation names sorted alphabetically for consistent display
func (sd *StatsDisplay) getSortedOperations(counts map[string]int64) []string {
	operations := make([]string, 0, len(counts))
	for op := range counts {
		operations = append(operations, op)
	}
	sort.Strings(operations)
	return operations
}

// ClearDisplay clears the terminal display (useful for tests or manual control)
func (sd *StatsDisplay) ClearDisplay() {
	fmt.Print("\033[2J\033[H") // clear screen and move cursor to top
}

// SetQuiet enables or disables quiet mode during test execution
func (sd *StatsDisplay) SetQuiet(quiet bool) {
	sd.config.Quiet = quiet
}

// example usage function showing how to integrate with existing iotest code
func ExampleDisplayUsage() {
	// create stats collector
	collector := NewStatsCollector(100, 10, true)

	// create display with configuration
	displayConfig := DisplayConfig{
		UpdateInterval: 1 * time.Second,
		ShowLatency:    true,
		ShowProgress:   true,
		TestDuration:   30 * time.Second,
		Quiet:          false,
	}
	display := NewStatsDisplay(collector, displayConfig)

	// start both collector and display
	collector.Start()
	display.Start()

	// run test here...
	// workers would be sending updates to collector
	// display automatically shows live updates

	// when test completes
	collector.Stop()
	display.Stop()

	// show final summary
	finalStats := collector.GetFinalStats()
	display.ShowFinalSummary(finalStats)
}
