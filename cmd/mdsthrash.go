/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/jessegalley/iolyzer/internal/iotest"
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
			os.Exit(2)
		}
		config.TestDir = testDir

		// validate all cli flags
		err := validateMDSThrashParameters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing opts: %v\n", err)
			os.Exit(3)
		}

		// execute this test
		if err := runMDSThrash(); err != nil {
			fmt.Fprintf(os.Stderr, "failed: %v\n", err)
			os.Exit(4)
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

// validateMDSThrashParameters validates mdsthrash-specific command line parameters
func validateMDSThrashParameters() error {
	//validate dir count
	if dirCount < 1 {
		return fmt.Errorf("--dirs must be a positive integer, got %d", dirCount)
	}
	fmt.Println(dirCount)
	config.DirCount = dirCount

	//validate files per batch
	if filesPerBatch < 1 {
		return fmt.Errorf("--files-per-batch must be a positive integer, got %d", filesPerBatch)
	}
	config.FilesPerBatch = filesPerBatch

	// validate files per move
	if filesPerMove < 0 {
		return fmt.Errorf("--files-per-move must be a positive integer, got %d", filesPerMove)
	}
	config.FilesPerMove = filesPerMove

	// validate fsync close frequence
	if fsyncCloseFreq < 0 || fsyncCloseFreq > 100 {
		return fmt.Errorf("--fsync-close-freq must be beteween 0-100, got %d", fsyncCloseFreq)
	}
	config.FsyncCloseFreq = fsyncCloseFreq

	return nil
}

// runMDSThrash actually executes the test
func runMDSThrash() error {
	// use config from package scope
	iotest, err := iotest.New(config)
	if err != nil {
		return fmt.Errorf("couldn't initialize iotest, %v", err)
	}

	err = iotest.StartMDSThrash()
	if err != nil {
		return fmt.Errorf("couldn't run iotest, %v", err)
	}

	return nil
}
