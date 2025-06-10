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
`,
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
		return nil
}

// validateMDSThrashParameters validates mdsthrash-specific command line parameters
func validateMDSThrashParameters() error {
  //NYI
  return nil
}
