/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	// "time"

	"github.com/jessegalley/iolyzer/internal/layout"
	"github.com/jessegalley/iolyzer/internal/output"
	"github.com/jessegalley/iolyzer/internal/runners"
	"github.com/spf13/cobra"
)

var (
	rwmix int // read/write mix percentage
)

// mixedrwCmd represents the mixedrw command
var mixedrwCmd = &cobra.Command{
	Use:   "mixedrw [test_path]",
	Short: "Perform mixed read/write tests.",
	Long: `Test mixed read/write performance with one file per thread. 
If test_path is not provided, iolyzer will try to make and use ./iolyzer_test`,
	Args: cobra.MaximumNArgs(1),
	Run: func(cmd *cobra.Command, args []string) {
		// if positional arg was given, override the
		// default test dir set in root
		if len(args) == 1 {
			testDir = args[0]
		}

		// validate all cli flags for this command
		err := validateMixedRWParameters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
			os.Exit(2)
		}

		// validate that the testdir (default or arg) exists
		// and is writable by the calling user. create it
		// if possible
		if err := ensureWritableDirectory(testDir); err != nil {
			fmt.Fprintf(os.Stderr, "error: %v\n", err)
		}

		// execute this test
		runMixedRW(testDir)
	},
}

func init() {
	rootCmd.AddCommand(mixedrwCmd)
	mixedrwCmd.PersistentFlags().Int64VarP(&fileSize, "size", "s", 100, "size of each test file in MiB")
	mixedrwCmd.Flags().IntVar(&rwmix, "rwmix", 75, "percentage of operations that should be reads (0-100)")
}

func validateMixedRWParameters() error {

	//TODO: propagate config refactor down into this test code

	// validate file size
	if fileSize <= 0 {
		return fmt.Errorf("--size must be a positive integer, got %v", fileSize)
	}
	config.FileSize = fileSize

	if rwmix < 0 || rwmix > 100 {
		return fmt.Errorf("--rwmix must be between 1 and 100, got %d", rwmix)
	}
	config.ReadWriteMix = rwmix

	return nil
}

func runMixedRW(testDir string) {
	var testFiles []string

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
			fmt.Fprintf(os.Stderr, "failed to create test file %s: %v", workerFile, err)
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
		testDuration,
	)

	if err != nil {
		fmt.Fprintf(os.Stderr, "test failed  %v", err)
	}

	// format and output the results
	format, err := output.ValidateFormat(outFmt)
	if err != nil {
		//TODO: this is also checked in the root before
		//      continuing so really we should never get here
		//      maybe there is a better way to handle this
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}

	output, err := output.FormatResult(result, format)
	if err != nil {
		fmt.Fprintf(os.Stderr, "failed for format results %v", err)
	}

	// print the formatted output
	fmt.Print(output)
}
