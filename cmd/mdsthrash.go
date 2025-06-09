/*
Copyright © 2025 jesse galley <jesse@jessegalley.net>
*/
package cmd

import (
	"fmt"
	"os"

	"github.com/jessegalley/iolyzer/internal/runners"
	"github.com/spf13/cobra"
)

// mdsthrashCmd represents the mdsthrash command
var mdsthrashCmd = &cobra.Command{
	Use:   "mdsthrash",
	Short: "Simulate a workload with a LOT of small file create/move/unlinks.",
	Long: `Attempts to create hundreds of small files per second, write tiny amounts to them,
  mass move them to another dir, and later attmpts to unlink files created previously.

  This test is designed around the particulars of the Ceph MDS, in particular, it tries to 
  make the MDS feel as much pain as possible. Many small files will cause MDS trim issues,
  and if iolyzer is run from multiple hosts, it almost guarantees that write capabilities 
  on parent dentries will thrash between hosts constantly.

  Running this test from only one client will not thrash capabilities, and will likely not 
  give any meaningful results.`,
	Run: func(cmd *cobra.Command, args []string) {
		fmt.Println("mdsthrash called")
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
		}

		// execute this test
		runMDSThrash(testDir)
	},
}

func init() {
	rootCmd.AddCommand(mdsthrashCmd)

	mdsthrashCmd.PersistentFlags().Int64VarP(&fileSize, "size", "s", 4, "size of each test file in KiB")
  // args should be created here from the mdsthrash runner code TestConfig struct (if the same args don't already exist in root scope)
}

func runMDSThrash(testDir string) {

	//  ensure dirs are created _testdir_/{in,out}/{1..N} where N is num_dirs
  //  not sure if existing layout code can be used for this  

  c := runners.TestConfig{TestDir: testDir} // configs should be populated based on args 
  result, err := runners.MDSThrashTest(c)
  if err != nil {
    // do something 
  }

  // display result 
  fmt.Println(result)
}
