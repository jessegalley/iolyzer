/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"
	// "time"

	"github.com/jessegalley/iolyzer/internal/output"
	"github.com/jessegalley/iolyzer/internal/runners"
	"github.com/spf13/cobra"
)

var (
	// mdsthrash-specific flags
	dirCount       int // number of directories to create in in/ and out/
	filesPerBatch  int // files to create before moving to next directory
	filesPerMove   int // files to create before moving them to output
	fsyncCloseFreq int // percentage of files that get fsync before close
	maxReadFiles   int // maximum files to read in unlinking phase
)

// mdsthrashCmd represents the mdsthrash command
var mdsthrashCmd = &cobra.Command{
	Use:   "mdsthrash [test_path]",
	Short: "Simulate a workload with a LOT of small file create/move/unlinks.",
	Long: `Attempts to create hundreds of small files per second, write tiny amounts to them,
mass move them to another dir, and later attempts to unlink files created previously.

This test is designed around the particulars of the Ceph MDS, in particular, it tries to 
make the MDS feel as much pain as possible. Many small files will cause MDS trim issues,
and if iolyzer is run from multiple hosts, it almost guarantees that write capabilities 
on parent dentries will thrash between hosts constantly.

Running this test from only one client will not thrash capabilities, and will likely not 
give any meaningful results.

The test creates a directory structure like:
  <test_path>/in/{01..NN}/
  <test_path>/out/{01..NN}/

Where NN is the number specified by --dirs.

Each worker goroutine will:
1. Create small files in random in/XX directories
2. Write data to each file
3. Move files to corresponding out/XX directories  
4. Read and delete files from different out/XX directories
5. Repeat until test duration expires

This pattern maximizes metadata operations and capability thrashing in distributed filesystems.`,
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
		if err := runMDSThrash(testDir); err != nil {
			fmt.Fprintf(os.Stderr, "test failed: %v\n", err)
			os.Exit(1)
		}
	},
}

func init() {
	rootCmd.AddCommand(mdsthrashCmd)

	// file size flag - override default for mdsthrash (small files)
	mdsthrashCmd.PersistentFlags().Int64VarP(&fileSize, "size", "s", 4, "size of each test file in KiB")

	// mdsthrash-specific flags
	mdsthrashCmd.Flags().IntVar(&dirCount, "dirs", 8, "number of directories to create in in/ and out/ (01..NN)")
	mdsthrashCmd.Flags().IntVar(&filesPerBatch, "files-per-batch", 10, "number of files to create in a directory before moving to next")
	mdsthrashCmd.Flags().IntVar(&filesPerMove, "files-per-move", 0, "number of files to create before moving them (0 = move all at end of batch)")
	mdsthrashCmd.Flags().IntVar(&fsyncCloseFreq, "fsync-close-freq", 0, "percentage of files that get fsync() before close() (0-100)")
	mdsthrashCmd.Flags().IntVar(&maxReadFiles, "max-read-files", 0, "maximum number of files to read/unlink per phase (0 = unlimited)")
}

func runMDSThrash(testDir string) error {
	// validate output format
	format, err := output.ValidateFormat(outFmt)
	if err != nil {
		return fmt.Errorf("invalid output format: %w", err)
	}

	// validate mdsthrash-specific parameters
	if err := validateMDSThrashParameters(); err != nil {
		return fmt.Errorf("invalid parameters: %w", err)
	}

	// announce test start with configuration details
	fmt.Fprintf(os.Stderr, "starting MDS thrash test:\n")
	fmt.Fprintf(os.Stderr, "  workers: %d\n", parallelJobs)
	fmt.Fprintf(os.Stderr, "  directories: %d (in/01-%02d, out/01-%02d)\n", dirCount, dirCount, dirCount)
	fmt.Fprintf(os.Stderr, "  file size: %d KiB\n", fileSize)
	fmt.Fprintf(os.Stderr, "  files per batch: %d\n", filesPerBatch)
	if filesPerMove > 0 {
		fmt.Fprintf(os.Stderr, "  files per move: %d\n", filesPerMove)
	}
	fmt.Fprintf(os.Stderr, "  duration: %v\n", testDuration)
	fmt.Fprintf(os.Stderr, "  block size: %d bytes\n", blockSize)
	if fsyncFreq > 0 {
		fmt.Fprintf(os.Stderr, "  fsync frequency: every %d writes\n", fsyncFreq)
	}
	if fsyncCloseFreq > 0 {
		fmt.Fprintf(os.Stderr, "  fsync before close: %d%% of files\n", fsyncCloseFreq)
	}
	if maxReadFiles > 0 {
		fmt.Fprintf(os.Stderr, "  max read files: %d per phase\n", maxReadFiles)
	}
	fmt.Fprintf(os.Stderr, "\n")

	// create test configuration using constructor with defaults
	config := runners.NewTestConfig()

	// override defaults with command line values
	config.ParallelJobs = parallelJobs
	config.TestDir = testDir
	config.DirCount = dirCount
	config.FileSize = fileSize * 1024 // convert KiB to bytes
	config.FilesPerBatch = filesPerBatch
	config.FilesPerMove = filesPerMove
	config.FsyncFreq = fsyncFreq
	config.FsyncCloseFreq = fsyncCloseFreq
	config.BlockSize = blockSize
	config.MaxReadFiles = maxReadFiles
	config.Duration = testDuration

	// execute the MDS thrash test and get detailed results
	mdsThrashResult, err := runners.MDSThrashTestDetailed(config)
	if err != nil {
		return fmt.Errorf("MDS thrash test failed: %w", err)
	}

	// convert detailed result to output package format to avoid circular imports
	outputResult := output.MDSThrashResult{
		CreateCount:  mdsThrashResult.CreateCount,
		WriteCount:   mdsThrashResult.WriteCount,
		ReadCount:    mdsThrashResult.ReadCount,
		MoveCount:    mdsThrashResult.MoveCount,
		UnlinkCount:  mdsThrashResult.UnlinkCount,
		CreateErrors: mdsThrashResult.CreateErrors,
		WriteErrors:  mdsThrashResult.WriteErrors,
		ReadErrors:   mdsThrashResult.ReadErrors,
		MoveErrors:   mdsThrashResult.MoveErrors,
		UnlinkErrors: mdsThrashResult.UnlinkErrors,
		BytesRead:    mdsThrashResult.BytesRead,
		BytesWritten: mdsThrashResult.BytesWritten,
		Duration:     mdsThrashResult.Duration,
	}

	// copy latency statistics
	outputResult.CreateLatency = convertLatencyStats(mdsThrashResult.CreateLatency)
	outputResult.WriteLatency = convertLatencyStats(mdsThrashResult.WriteLatency)
	outputResult.ReadLatency = convertLatencyStats(mdsThrashResult.ReadLatency)
	outputResult.MoveLatency = convertLatencyStats(mdsThrashResult.MoveLatency)
	outputResult.UnlinkLatency = convertLatencyStats(mdsThrashResult.UnlinkLatency)

	// format and output the results using the specialized MDS thrash formatter
	outputStr, err := output.FormatMDSThrashResult(outputResult, format)
	if err != nil {
		return fmt.Errorf("failed to format results: %w", err)
	}

	// print the formatted output
	fmt.Print(outputStr)

	return nil
}

// convertLatencyStats converts runners.LatencyStats to output.LatencyStats
// this is needed to avoid circular import dependencies
func convertLatencyStats(src runners.LatencyStats) output.LatencyStats {
	return output.LatencyStats{
		Min:   src.Min,
		Max:   src.Max,
		Avg:   src.Avg,
		Count: src.Count,
	}
}

// validateMDSThrashParameters validates mdsthrash-specific command line parameters
func validateMDSThrashParameters() error {
	//NYI
	return nil
}

// /*
// Copyright © 2025 jesse galley <jesse@jessegalley.net>
// */
// package cmd
//
// import (
// 	"fmt"
// 	"os"
//
// 	"github.com/jessegalley/iolyzer/internal/runners"
// 	"github.com/spf13/cobra"
// )
//
// // mdsthrashCmd represents the mdsthrash command
// var mdsthrashCmd = &cobra.Command{
// 	Use:   "mdsthrash",
// 	Short: "Simulate a workload with a LOT of small file create/move/unlinks.",
// 	Long: `Attempts to create hundreds of small files per second, write tiny amounts to them,
//   mass move them to another dir, and later attmpts to unlink files created previously.
//
//   This test is designed around the particulars of the Ceph MDS, in particular, it tries to
//   make the MDS feel as much pain as possible. Many small files will cause MDS trim issues,
//   and if iolyzer is run from multiple hosts, it almost guarantees that write capabilities
//   on parent dentries will thrash between hosts constantly.
//
//   Running this test from only one client will not thrash capabilities, and will likely not
//   give any meaningful results.`,
// 	Run: func(cmd *cobra.Command, args []string) {
// 		fmt.Println("mdsthrash called")
// 		// if positional arg was given, override the
// 		// default test dir set in root
// 		if len(args) == 1 {
// 			testDir = args[0]
// 		}
//
// 		// validate that the testdir (default or arg) exists
// 		// and is writable by the calling user. create it
// 		// if possible
// 		if err := ensureWritableDirectory(testDir); err != nil {
// 			fmt.Fprintf(os.Stderr, "error: %v\n", err)
// 		}
//
// 		// execute this test
// 		runMDSThrash(testDir)
// 	},
// }
//
// func init() {
// 	rootCmd.AddCommand(mdsthrashCmd)
//
// 	mdsthrashCmd.PersistentFlags().Int64VarP(&fileSize, "size", "s", 4, "size of each test file in KiB")
//   // args should be created here from the mdsthrash runner code TestConfig struct (if the same args don't already exist in root scope)
// }
//
// func runMDSThrash(testDir string) {
//
// 	//  ensure dirs are created _testdir_/{in,out}/{1..N} where N is num_dirs
//   //  not sure if existing layout code can be used for this
//
//   c := runners.TestConfig{TestDir: testDir} // configs should be populated based on args
//   result, err := runners.MDSThrashTest(c)
//   if err != nil {
//     // do something
//   }
//
//   // display result
//   fmt.Println(result)
// }
