/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	// "github.com/jessegalley/iolyzer/internal/runners"
	"github.com/jessegalley/iolyzer/internal/layout"
	"github.com/jessegalley/iolyzer/internal/output"
	"github.com/jessegalley/iolyzer/internal/runners"
	"github.com/spf13/cobra"
)

var (
    rwmix int        // read/write mix percentage
)

// randrwCmd represents the randrw command
var randrwCmd = &cobra.Command{
	Use:   "randrw [test_path]",
	Short: "Perform mixed read/write tests.",
	Long: `Test mixed read/write performance with one file per thread. 
If test_path is not provided, iolyzer will try to make and use ./iolyzer_test`,
  Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
    err := validateParameters()
    if err != nil {
      fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
      os.Exit(2)
    }

    var testDir string
    if len(args) == 1 {
      testDir = args[0]
    } else {
      testDir = "./iolyzer_test/"
    }
    if err := ensureWritableDirectory(testDir); err != nil {
      fmt.Fprintf(os.Stderr, "error: %v\n", err)
    }
    runRandRW(testDir)
	},
}

func init() {
	rootCmd.AddCommand(randrwCmd)

	// Here you will define your flags and configuration settings.

	// Cobra supports Persistent Flags which will work for this command
	// and all subcommands, e.g.:
	// randrwCmd.PersistentFlags().String("foo", "", "A help for foo")

	// Cobra supports local flags which will only run when this command
	// is called directly, e.g.:
	// randrwCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
  randrwCmd.Flags().IntVar(&rwmix, "rwmix", 75, "percentage of operations that should be reads (0-100)")
}

func runRandRW(testDir string) {
    // create slice to track test files
    var testFiles []string

    // validate output format
    format, err := output.ValidateFormat(outFmt)
    if err != nil {
      fmt.Fprintln(os.Stderr, err)
      os.Exit(1)
    }

    // create test files for each worker
    for i := 0; i < parallelJobs; i++ {
      // generate unique file name for this worker
      workerFile := fmt.Sprintf("%s_%d.dat", fileName, i)

      // create the full path by joining the test directory and worker file name
      fullPath := filepath.Join(testDir, workerFile)

      // ensure file path is absolute
      absPath, err := filepath.Abs(fullPath)
      if err != nil {
        fmt.Fprintf(os.Stderr, "failed to get absolute path: %v", err)
      }

      // create the test file
      if err := layout.LayoutTestFile(absPath, int(fileSize*1024*1024), reinitFile); err != nil {
        fmt.Fprintf(os.Stderr, "failed to create test file %s: %v", workerFile,  err)
      }

      // add file to tracking slice
      testFiles = append(testFiles, absPath)
    }

    // announce test start
    fmt.Fprintf(os.Stderr, "starting mixed R/W test with %d workers (%d%% reads)\n", parallelJobs, rwmix)

    // execute the mixed read/write test
    result, err := runners.MixedRWTest(
        testFiles,
        blockSize,
        rwmix,
        directIO,
        oSync,
        fsyncFreq,
        time.Duration(testDuration)*time.Second,
    )

    if err != nil {
        fmt.Fprintf(os.Stderr, "test failed  %v", err)
    }

    // format and output the results
    output, err := output.FormatResult(result, format)
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed for format results %v", err)
    }

    // print the formatted output
    fmt.Print(output)
}

// validateParameters checks all command line parameters for validity
func validateParameters() error {
    // validate read/write mix percentage
    if rwmix < 0 || rwmix > 100 {
        return fmt.Errorf("rwmix must be between 0 and 100, got %d", rwmix)
    }

    // validate metadata mix percentage
    if metamix < 0 || metamix > 99 {
        return fmt.Errorf("metamix must be between 0 and 99, got %d", metamix)
    }

    // validate number of parallel jobs
    if parallelJobs < 1 {
        return fmt.Errorf("parallel-jobs must be at least 1, got %d", parallelJobs)
    }

    // validate block size
    if blockSize <= 0 {
        return fmt.Errorf("block size must be positive, got %d", blockSize)
    }

    // validate file size
    if fileSize <= 0 {
        return fmt.Errorf("file size must be positive, got %d", fileSize)
    }

    return nil
}

func ensureWritableDirectory(dirPath string) error {
    // First check if directory exists
    if info, err := os.Stat(dirPath); err == nil {
        // Directory exists, check if it's a directory and writable
        if !info.IsDir() {
            return fmt.Errorf("%s exists but is not a directory", dirPath)
        }
        
        // Try to create a temporary file to test writeability
        testFile := filepath.Join(dirPath, ".write_test")
        if f, err := os.Create(testFile); err != nil {
            return fmt.Errorf("directory %s exists but is not writable: %v", dirPath, err)
        } else {
            f.Close()
            os.Remove(testFile)
        }
        
        return nil
    } else if !os.IsNotExist(err) {
        // Error other than "not exists" occurred
        return fmt.Errorf("failed to check directory %s: %v", dirPath, err)
    }
    
    // Directory doesn't exist, try to create it
    if err := os.MkdirAll(dirPath, 0755); err != nil {
        return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
    }
    
    return nil
}
