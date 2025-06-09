// mdsthrash_output.go contains output formatting specific to MDS thrash test results
package output

import (
	"encoding/json"
	"fmt"
	"strings"
	"time"
)

// MDSThrashResult represents the detailed results from an MDS thrash test
// this is a copy of the struct from runners package to avoid circular imports
type MDSThrashResult struct {
	// basic operation counts
	CreateCount  int64
	WriteCount   int64
	ReadCount    int64
	MoveCount    int64
	UnlinkCount  int64
  FsyncCount   int64
	
	// error counts
	CreateErrors int64
	WriteErrors  int64
	ReadErrors   int64
	MoveErrors   int64
	UnlinkErrors int64
  FsyncErrors  int64
	
	// byte transfer metrics
	BytesRead    int64
	BytesWritten int64
	
	// latency metrics for each operation type
	CreateLatency  LatencyStats
	WriteLatency   LatencyStats
	ReadLatency    LatencyStats
	MoveLatency    LatencyStats
	UnlinkLatency  LatencyStats
	FsyncLatency   LatencyStats
	
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

// FormatMDSThrashResult formats MDSThrashResult according to the specified format
func FormatMDSThrashResult(result MDSThrashResult, format OutputFormat) (string, error) {
	// calculate rates and throughput metrics
	seconds := result.Duration.Seconds()
	
	// operation rates (operations per second)
	createRate := float64(result.CreateCount) / seconds
	writeRate := float64(result.WriteCount) / seconds
	readRate := float64(result.ReadCount) / seconds
	moveRate := float64(result.MoveCount) / seconds
	unlinkRate := float64(result.UnlinkCount) / seconds
	
	// error rates
	createErrorRate := float64(result.CreateErrors) / seconds
	writeErrorRate := float64(result.WriteErrors) / seconds
	readErrorRate := float64(result.ReadErrors) / seconds
	moveErrorRate := float64(result.MoveErrors) / seconds
	unlinkErrorRate := float64(result.UnlinkErrors) / seconds
	
	// throughput metrics (MB/s)
	readThroughput := float64(result.BytesRead) / seconds / (1024 * 1024)
	writeThroughput := float64(result.BytesWritten) / seconds / (1024 * 1024)
	
	// handle each output format
	switch format {
	case TableFormat:
		return formatMDSThrashTable(result, createRate, writeRate, readRate, moveRate, unlinkRate,
			createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
			readThroughput, writeThroughput), nil
			
	case JSONFormat:
		return formatMDSThrashJSON(result, createRate, writeRate, readRate, moveRate, unlinkRate,
			createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
			readThroughput, writeThroughput)
			
	case FlatFormat:
		return formatMDSThrashFlat(createRate, writeRate, readRate, moveRate, unlinkRate,
			createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
			readThroughput, writeThroughput), nil
			
	default:
		return "", fmt.Errorf("unsupported output format: %s", format)
	}
}

// formatMDSThrashTable creates a human-readable table format for MDS thrash results
func formatMDSThrashTable(result MDSThrashResult, createRate, writeRate, readRate, moveRate, unlinkRate,
	createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
	readThroughput, writeThroughput float64) string {
	
	var sb strings.Builder
	
	// write main metrics table
	sb.WriteString(fmt.Sprintf("\nMDS Thrash Test Results (Duration: %.1fs)\n", result.Duration.Seconds()))
	sb.WriteString(fmt.Sprintf("===========================================\n\n"))
	
	// operation counts and rates table
	sb.WriteString(fmt.Sprintf("%-12s %12s %12s %12s %12s\n", "Operation", "Count", "Rate/sec", "Errors", "Err/sec"))
	sb.WriteString(fmt.Sprintf("%-12s %12s %12s %12s %12s\n", "---------", "-----", "--------", "------", "-------"))
	sb.WriteString(fmt.Sprintf("%-12s %12d %12.2f %12d %12.2f\n", "create", result.CreateCount, createRate, result.CreateErrors, createErrorRate))
	sb.WriteString(fmt.Sprintf("%-12s %12d %12.2f %12d %12.2f\n", "write", result.WriteCount, writeRate, result.WriteErrors, writeErrorRate))
	sb.WriteString(fmt.Sprintf("%-12s %12d %12.2f %12d %12.2f\n", "read", result.ReadCount, readRate, result.ReadErrors, readErrorRate))
	sb.WriteString(fmt.Sprintf("%-12s %12d %12.2f %12d %12.2f\n", "move", result.MoveCount, moveRate, result.MoveErrors, moveErrorRate))
	sb.WriteString(fmt.Sprintf("%-12s %12d %12.2f %12d %12.2f\n", "unlink", result.UnlinkCount, unlinkRate, result.UnlinkErrors, unlinkErrorRate))
	
	// throughput metrics
	sb.WriteString(fmt.Sprintf("\n%-12s %12s\n", "Throughput", "MB/sec"))
	sb.WriteString(fmt.Sprintf("%-12s %12s\n", "----------", "------"))
	sb.WriteString(fmt.Sprintf("%-12s %12.2f\n", "read", readThroughput))
	sb.WriteString(fmt.Sprintf("%-12s %12.2f\n", "write", writeThroughput))
	
	// latency summary table
	sb.WriteString(fmt.Sprintf("\n%-12s %12s %12s %12s %12s\n", "Latency", "Count", "Min (μs)", "Max (μs)", "Avg (μs)"))
	sb.WriteString(fmt.Sprintf("%-12s %12s %12s %12s %12s\n", "-------", "-----", "--------", "--------", "--------"))
	
	// format latency stats for each operation type
	formatLatencyRow(&sb, "create", result.CreateLatency)
	formatLatencyRow(&sb, "write", result.WriteLatency)
	formatLatencyRow(&sb, "read", result.ReadLatency)
	formatLatencyRow(&sb, "move", result.MoveLatency)
	formatLatencyRow(&sb, "unlink", result.UnlinkLatency)
	
	return sb.String()
}

// formatLatencyRow formats a single row of latency statistics
func formatLatencyRow(sb *strings.Builder, operation string, stats LatencyStats) {
	if stats.Count > 0 {
		minMicros := float64(stats.Min.Nanoseconds()) / 1000
		maxMicros := float64(stats.Max.Nanoseconds()) / 1000
		avgMicros := float64(stats.Avg.Nanoseconds()) / 1000
		sb.WriteString(fmt.Sprintf("%-12s %12d %12.1f %12.1f %12.1f\n", 
			operation, stats.Count, minMicros, maxMicros, avgMicros))
	} else {
		sb.WriteString(fmt.Sprintf("%-12s %12d %12s %12s %12s\n", 
			operation, 0, "-", "-", "-"))
	}
}

// formatMDSThrashJSON creates JSON format output for MDS thrash results
func formatMDSThrashJSON(result MDSThrashResult, createRate, writeRate, readRate, moveRate, unlinkRate,
	createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
	readThroughput, writeThroughput float64) (string, error) {
	
	// create structured result for JSON output
	jsonResult := struct {
		Duration  float64 `json:"duration_seconds"`
		Operations struct {
			Create struct {
				Count    int64   `json:"count"`
				Rate     float64 `json:"rate_per_sec"`
				Errors   int64   `json:"errors"`
				ErrorRate float64 `json:"error_rate_per_sec"`
				Latency  struct {
					Count int64   `json:"count"`
					Min   float64 `json:"min_microseconds"`
					Max   float64 `json:"max_microseconds"`
					Avg   float64 `json:"avg_microseconds"`
				} `json:"latency"`
			} `json:"create"`
			Write struct {
				Count    int64   `json:"count"`
				Rate     float64 `json:"rate_per_sec"`
				Errors   int64   `json:"errors"`
				ErrorRate float64 `json:"error_rate_per_sec"`
				Latency  struct {
					Count int64   `json:"count"`
					Min   float64 `json:"min_microseconds"`
					Max   float64 `json:"max_microseconds"`
					Avg   float64 `json:"avg_microseconds"`
				} `json:"latency"`
			} `json:"write"`
			Read struct {
				Count    int64   `json:"count"`
				Rate     float64 `json:"rate_per_sec"`
				Errors   int64   `json:"errors"`
				ErrorRate float64 `json:"error_rate_per_sec"`
				Latency  struct {
					Count int64   `json:"count"`
					Min   float64 `json:"min_microseconds"`
					Max   float64 `json:"max_microseconds"`
					Avg   float64 `json:"avg_microseconds"`
				} `json:"latency"`
			} `json:"read"`
			Move struct {
				Count    int64   `json:"count"`
				Rate     float64 `json:"rate_per_sec"`
				Errors   int64   `json:"errors"`
				ErrorRate float64 `json:"error_rate_per_sec"`
				Latency  struct {
					Count int64   `json:"count"`
					Min   float64 `json:"min_microseconds"`
					Max   float64 `json:"max_microseconds"`
					Avg   float64 `json:"avg_microseconds"`
				} `json:"latency"`
			} `json:"move"`
			Unlink struct {
				Count    int64   `json:"count"`
				Rate     float64 `json:"rate_per_sec"`
				Errors   int64   `json:"errors"`
				ErrorRate float64 `json:"error_rate_per_sec"`
				Latency  struct {
					Count int64   `json:"count"`
					Min   float64 `json:"min_microseconds"`
					Max   float64 `json:"max_microseconds"`
					Avg   float64 `json:"avg_microseconds"`
				} `json:"latency"`
			} `json:"unlink"`
		} `json:"operations"`
		Throughput struct {
			Read  float64 `json:"read_mb_per_sec"`
			Write float64 `json:"write_mb_per_sec"`
		} `json:"throughput"`
	}{
		Duration: result.Duration.Seconds(),
	}
	
	// populate create operation data
	jsonResult.Operations.Create.Count = result.CreateCount
	jsonResult.Operations.Create.Rate = createRate
	jsonResult.Operations.Create.Errors = result.CreateErrors
	jsonResult.Operations.Create.ErrorRate = createErrorRate
	populateLatencyJSON(&jsonResult.Operations.Create.Latency, result.CreateLatency)
	
	// populate write operation data
	jsonResult.Operations.Write.Count = result.WriteCount
	jsonResult.Operations.Write.Rate = writeRate
	jsonResult.Operations.Write.Errors = result.WriteErrors
	jsonResult.Operations.Write.ErrorRate = writeErrorRate
	populateLatencyJSON(&jsonResult.Operations.Write.Latency, result.WriteLatency)
	
	// populate read operation data
	jsonResult.Operations.Read.Count = result.ReadCount
	jsonResult.Operations.Read.Rate = readRate
	jsonResult.Operations.Read.Errors = result.ReadErrors
	jsonResult.Operations.Read.ErrorRate = readErrorRate
	populateLatencyJSON(&jsonResult.Operations.Read.Latency, result.ReadLatency)
	
	// populate move operation data
	jsonResult.Operations.Move.Count = result.MoveCount
	jsonResult.Operations.Move.Rate = moveRate
	jsonResult.Operations.Move.Errors = result.MoveErrors
	jsonResult.Operations.Move.ErrorRate = moveErrorRate
	populateLatencyJSON(&jsonResult.Operations.Move.Latency, result.MoveLatency)
	
	// populate unlink operation data
	jsonResult.Operations.Unlink.Count = result.UnlinkCount
	jsonResult.Operations.Unlink.Rate = unlinkRate
	jsonResult.Operations.Unlink.Errors = result.UnlinkErrors
	jsonResult.Operations.Unlink.ErrorRate = unlinkErrorRate
	populateLatencyJSON(&jsonResult.Operations.Unlink.Latency, result.UnlinkLatency)
	
	// populate throughput data
	jsonResult.Throughput.Read = readThroughput
	jsonResult.Throughput.Write = writeThroughput
	
	// marshal to JSON
	jsonBytes, err := json.MarshalIndent(jsonResult, "", "  ")
	if err != nil {
		return "", fmt.Errorf("failed to marshal JSON: %w", err)
	}
	
	return string(jsonBytes), nil
}

// populateLatencyJSON fills in latency data for JSON output
func populateLatencyJSON(target interface{}, stats LatencyStats) {
	// use type assertion to access the latency struct fields
	switch latency := target.(type) {
	case *struct {
		Count int64   `json:"count"`
		Min   float64 `json:"min_microseconds"`
		Max   float64 `json:"max_microseconds"`
		Avg   float64 `json:"avg_microseconds"`
	}:
		latency.Count = stats.Count
		if stats.Count > 0 {
			latency.Min = float64(stats.Min.Nanoseconds()) / 1000
			latency.Max = float64(stats.Max.Nanoseconds()) / 1000
			latency.Avg = float64(stats.Avg.Nanoseconds()) / 1000
		}
	}
}

// formatMDSThrashFlat creates flat space-separated format for MDS thrash results
func formatMDSThrashFlat(createRate, writeRate, readRate, moveRate, unlinkRate,
	createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
	readThroughput, writeThroughput float64) string {
	
	// return all key metrics as space-separated values
	return fmt.Sprintf("%.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f %.2f\n",
		createRate, writeRate, readRate, moveRate, unlinkRate,
		createErrorRate, writeErrorRate, readErrorRate, moveErrorRate, unlinkErrorRate,
		readThroughput, writeThroughput)
}
