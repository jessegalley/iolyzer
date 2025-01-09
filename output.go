package iolyzer

import (
    "encoding/json"
    "fmt"
    "strings"
)

// OutputFormat represents the supported output format types
type OutputFormat string

// supported output format constants
const (
    // table format outputs results in a human-readable table
    TableFormat OutputFormat = "table"
    
    // json format outputs results as a json object
    JSONFormat OutputFormat = "json"
    
    // flat format outputs results as space-separated values
    FlatFormat OutputFormat = "flat"
)

// FormatResult formats a TestResult according to the specified format
func FormatResult(result TestResult, format OutputFormat) (string, error) {
    // calculate derived metrics from the raw test results
    readIOPS := float64(result.ReadCount) / result.Duration.Seconds()
    writeIOPS := float64(result.WriteCount) / result.Duration.Seconds()
    readThroughput := float64(result.BytesRead) / result.Duration.Seconds() / (1024 * 1024)
    writeThroughput := float64(result.BytesWritten) / result.Duration.Seconds() / (1024 * 1024)

    // handle each output format
    switch format {
    case TableFormat:
        // create string builder for table output
        var sb strings.Builder
        
        // write table header
        sb.WriteString(fmt.Sprintf("\n%8s  %12s  %12s\n", "", "IOPS", "BW (MB/s)"))
        
        // write read metrics row
        sb.WriteString(fmt.Sprintf("%8s  %12.2f  %12.2f\n", "read", readIOPS, readThroughput))
        
        // write write metrics row
        sb.WriteString(fmt.Sprintf("%8s  %12.2f  %12.2f\n", "write", writeIOPS, writeThroughput))
        
        return sb.String(), nil

    case JSONFormat:
        // create a struct to hold the formatted metrics
        type formattedResult struct {
            Read struct {
                IOPS        float64 `json:"iops"`
                Throughput float64 `json:"throughput_mbs"`
            } `json:"read"`
            Write struct {
                IOPS        float64 `json:"iops"`
                Throughput float64 `json:"throughput_mbs"`
            } `json:"write"`
            Duration float64 `json:"duration_seconds"`
        }

        // populate the formatted result struct
        fr := formattedResult{}
        fr.Read.IOPS = readIOPS
        fr.Read.Throughput = readThroughput
        fr.Write.IOPS = writeIOPS
        fr.Write.Throughput = writeThroughput
        fr.Duration = result.Duration.Seconds()

        // marshal the result to json
        jsonBytes, err := json.MarshalIndent(fr, "", "  ")
        if err != nil {
            return "", fmt.Errorf("failed to marshal json: %w", err)
        }

        return string(jsonBytes), nil

    case FlatFormat:
        // return space-separated values with no headers
        return fmt.Sprintf("%.2f %.2f %.2f %.2f\n", 
            readIOPS, readThroughput, writeIOPS, writeThroughput), nil

    default:
        // return error for unsupported format
        return "", fmt.Errorf("unsupported output format: %s", format)
    }
}

// ValidateFormat checks if the provided format string is a valid output format
func ValidateFormat(format string) (OutputFormat, error) {
    // convert format to OutputFormat type
    f := OutputFormat(strings.ToLower(format))

    // check if format is supported
    switch f {
    case TableFormat, JSONFormat, FlatFormat:
        return f, nil
    default:
        return "", fmt.Errorf("invalid format '%s'. supported formats are: table, json, flat", format)
    }
}
