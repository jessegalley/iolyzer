/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/spf13/cobra"
)

// program flags defined as global variables for access across functions
var (
    fileSize int64   // size of test files in megabytes
    fileName string  // base name for test files
    blockSize int    // block size for io operations in kilobytes
    testDuration int // duration of test in seconds
    parallelJobs int // number of parallel jobs
    directIO bool    // whether to use direct io
    oSync bool       // whether to use O_SYNC
    fsyncFreq int    // fsync frequency
    outFmt string    // output format
    reinitFile bool  // whether to reinitialize existing test files
    version bool     // print version and exit
    metamix int      // metadata test
)

// program info const 
const progVersion string = "0.2.0"
const progAuthor string = "jesse galley <jesse@jessegalley.net>"

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "iolyzer",
	Short: "Test the i/o performance of filesystems.",
	Long: `TBD`,
  PersistentPreRun: func(cmd *cobra.Command, args []string) {
    // check if version flag was set
    if version {
      fmt.Printf("iolyzer v%s\njesse@jessegalley.net\ngithub.com/jessegalley/iolyzer\n", progVersion)
      os.Exit(1)
    }
  },
	// Uncomment the following line if your bare application
	// has an action associated with it:
	// Run: func(cmd *cobra.Command, args []string) { },
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
	// Here you will define your flags and configuration settings.
	// Cobra supports persistent flags, which, if defined here,
	// will be global for your application.

	// rootCmd.PersistentFlags().StringVar(&cfgFile, "config", "", "config file (default is $HOME/.iolyzer.yaml)")

	// Cobra also supports local flags, which will only run
	// when this action is called directly.
  // rootCmd.Flags().BoolP("toggle", "t", false, "Help message for toggle")
  // define command line flags, writing values to our global variables
  rootCmd.PersistentFlags().Int64VarP(&fileSize, "size", "s", 100, "size of each test file in megabytes")
  rootCmd.PersistentFlags().StringVar(&fileName, "file", "iolyzer_test", "base name for test files")
  rootCmd.PersistentFlags().IntVarP(&blockSize, "block", "b", 4096, "block size for io operations in bytes")
  rootCmd.PersistentFlags().IntVarP(&testDuration, "runtime", "t",  10, "duration of test in seconds")
  rootCmd.PersistentFlags().IntVarP(&parallelJobs, "parallel-jobs", "P", 1, "number of parallel jobs")
  rootCmd.PersistentFlags().BoolVarP(&directIO, "direct", "d", false, "use direct io (o_direct)")
  rootCmd.PersistentFlags().BoolVar(&oSync, "osync", false, "use O_SYNC for writes")
  rootCmd.PersistentFlags().IntVar(&fsyncFreq, "fsync", 0, "call fsync after this many writes (0 disables)")
  rootCmd.PersistentFlags().StringVar(&outFmt, "format", "table", "output format (table, json, or flat)")
  rootCmd.PersistentFlags().BoolVar(&reinitFile, "reinit", false, "reinitialize test files even if they already exist")
  rootCmd.PersistentFlags().BoolVarP(&version, "version", "V", false, "print version and exit")
  rootCmd.PersistentFlags().IntVarP(&metamix, "metamix", "m", 0, "how much metadata ops to mix in (0 is no md ops)")


}


