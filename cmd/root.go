/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"github.com/spf13/cobra"
)

// program flags defined as global variables for access across functions
var (
	testDir      string // directory in which to make test files
	fileSize     int64  // size of test files in bytes  
	fileName     string // base name for test files
	blockSize    int    // block size for io operations in bytes 
	// testDuration int    // duration of test in seconds
	testDuration time.Duration // duration of test 
	parallelJobs int    // number of parallel jobs
	directIO     bool   // whether to use direct io
	oSync        bool   // whether to use O_SYNC
	fsyncFreq    int    // fsync frequency
	outFmt       string // output format
	reinitFile   bool   // whether to reinitialize existing test files
	version      bool   // print version and exit
)

// program info const
const progVersion string = "0.2.0"
const progAuthor string = "jesse galley <jesse@jessegalley.net>"

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "iolyzer",
	Short: "Test the i/o performance of filesystems.",
	Long:  `TBD`,
	PersistentPreRun: func(cmd *cobra.Command, args []string) {
		// check if version flag was set
		if version {
			fmt.Printf("iolyzer v%s\njesse@jessegalley.net\ngithub.com/jessegalley/iolyzer\n", progVersion)
			os.Exit(1)
		}

		// validate all cli flags
		err := validateParameters()
		if err != nil {
			fmt.Fprintf(os.Stderr, "error parsing flags: %v\n", err)
			os.Exit(2)
		}

		// set the default test dir
		testDir = "./iolyzer_test/"
	},
}

// Execute adds all child commands to the root command and sets flags appropriately.
// This is called by main.main(). It only needs to happen once to the rootCmd.
func Execute() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}

func init() {
  //TODO: convert block size and file size args to SI/IEC notation 
  //      then refactor where the size args are parsed (root vs cmd)

	rootCmd.PersistentFlags().StringVar(&fileName, "file", "iolyzer_test", "base name for test files")
	rootCmd.PersistentFlags().IntVarP(&blockSize, "block", "b", 4096, "block size for io operations in bytes")
	// rootCmd.PersistentFlags().IntVarP(&testDuration, "runtime", "t", 10, "duration of test in seconds")
  rootCmd.PersistentFlags().DurationVarP(&testDuration, "runtime", "t", time.Second*30, "duration of test (e.g. 30s, 5m, 500ms) ")
	rootCmd.PersistentFlags().IntVarP(&parallelJobs, "parallel-jobs", "P", 1, "number of parallel jobs")
	rootCmd.PersistentFlags().BoolVarP(&directIO, "direct", "d", false, "use direct io (o_direct)")
	rootCmd.PersistentFlags().BoolVar(&oSync, "osync", false, "use O_SYNC for writes")
	rootCmd.PersistentFlags().IntVar(&fsyncFreq, "fsync", 0, "call fsync after this many writes (0 disables)")
	rootCmd.PersistentFlags().StringVar(&outFmt, "format", "table", "output format (table, json, or flat)")
	rootCmd.PersistentFlags().BoolVar(&reinitFile, "reinit", false, "reinitialize test files even if they already exist")
	rootCmd.PersistentFlags().BoolVarP(&version, "version", "V", false, "print version and exit")
}

// validateParameters checks all command line parameters for validity
func validateParameters() error {
	// validate read/write mix percentage
	if rwmix < 0 || rwmix > 100 {
		return fmt.Errorf("rwmix must be between 0 and 100, got %d", rwmix)
	}

	// validate metadata mix percentage
	// if metamix < 0 || metamix > 99 {
	//     return fmt.Errorf("metamix must be between 0 and 99, got %d", metamix)
	// }

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
	// first check if directory exists
	if info, err := os.Stat(dirPath); err == nil {
		// directory exists, check if it's a directory and writable
		if !info.IsDir() {
			return fmt.Errorf("%s exists but is not a directory", dirPath)
		}

		// try to create a temporary file to test writeability
		testFile := filepath.Join(dirPath, ".write_test")
		if f, err := os.Create(testFile); err != nil {
			return fmt.Errorf("directory %s exists but is not writable: %v", dirPath, err)
		} else {
			f.Close()
			os.Remove(testFile)
		}

		return nil
	} else if !os.IsNotExist(err) {
		// error other than "not exists" occurred
		return fmt.Errorf("failed to check directory %s: %v", dirPath, err)
	}

	// directory doesn't exist, try to create it
	if err := os.MkdirAll(dirPath, 0755); err != nil {
		return fmt.Errorf("failed to create directory %s: %v", dirPath, err)
	}

	return nil
}
