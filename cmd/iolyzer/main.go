package main

import (
    "fmt"
    "os"
    "path/filepath"
    "time"

    "github.com/jessegalley/iolyzer"
    "github.com/spf13/pflag"
)

func main() {
    // define command line flags
    var (
        // size of test files in megabytes
        fileSize = pflag.Int64("size", 1024, "size of each test file in megabytes")
        
        // base name for test files
        fileName = pflag.String("file", "iolyzer_test", "base name for test files")
        
        // block size for io operations in kilobytes
        blockSize = pflag.Int("block", 4096, "block size for io operations in bytes")
        
        // duration of test in seconds
        testDuration = pflag.Int("runtime", 10, "duration of test in seconds")
        
        // number of parallel jobs
        parallelJobs = pflag.IntP("parallel-jobs", "P", 1, "number of parallel jobs")
        
        // read/write mix percentage
        rwmix = pflag.Int("rwmix", 50, "percentage of operations that should be reads (0-100)")
        
        // whether to use direct io
        directIO = pflag.Bool("direct", false, "use direct io (o_direct)")
        
        // whether to use O_SYNC
        oSync = pflag.Bool("osync", false, "use O_SYNC for writes")
        
        // fsync frequency
        fsyncFreq = pflag.Int("fsync", 0, "call fsync after this many writes (0 disables)")

        // output format
        outputFormat = pflag.String("format", "table", "output format (table, json, or flat)")

        // whether to reinitialize existing test files
        reinitFile = pflag.Bool("reinit", false, "reinitialize test files even if they already exist")
    )

    // parse command line flags
    pflag.Parse()

    // validate output format
    format, err := iolyzer.ValidateFormat(*outputFormat)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    // validate other parameters
    if *rwmix < 0 || *rwmix > 100 {
        fmt.Fprintf(os.Stderr, "rwmix must be between 0 and 100, got %d\n", *rwmix)
        os.Exit(1)
    }

    if *parallelJobs < 1 {
        fmt.Fprintf(os.Stderr, "parallel-jobs must be at least 1, got %d\n", *parallelJobs)
        os.Exit(1)
    }

    if *blockSize <= 0 {
        fmt.Fprintf(os.Stderr, "block size must be positive, got %d\n", *blockSize)
        os.Exit(1)
    }

    if *fileSize <= 0 {
        fmt.Fprintf(os.Stderr, "file size must be positive, got %d\n", *fileSize)
        os.Exit(1)
    }

    // create slice to track test files
    var testFiles []string

		// create test files for each worker
		for i := 0; i < *parallelJobs; i++ {
			// generate unique file name for this worker
			workerFile := fmt.Sprintf("%s_%d.dat", *fileName, i)

			// ensure file path is absolute
			absPath, err := filepath.Abs(workerFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to get absolute path: %v\n", err)
				os.Exit(1)
			}

			// create the test file
			err = iolyzer.LayoutTestFile(absPath, int(*fileSize*1024*1024), *reinitFile)
			if err != nil {
				fmt.Fprintf(os.Stderr, "failed to create test file %s: %v\n", workerFile, err)
				os.Exit(1)
			}

			// add file to tracking slice
			testFiles = append(testFiles, absPath)
		}
    // // create test files for each worker
    // for i := 0; i < *parallelJobs; i++ {
    //     // generate unique file name for this worker
    //     workerFile := fmt.Sprintf("%s_%d.dat", *fileName, i)
    //     
    //     // ensure file path is absolute
    //     absPath, err := filepath.Abs(workerFile)
    //     if err != nil {
    //         fmt.Fprintf(os.Stderr, "failed to get absolute path: %v\n", err)
    //         os.Exit(1)
    //     }
    //
    //     // create the test file
    //     err = iolyzer.LayoutTestFile(absPath, int(*fileSize*1024*1024))
    //     if err != nil {
    //         fmt.Fprintf(os.Stderr, "failed to create test file %s: %v\n", workerFile, err)
    //         os.Exit(1)
    //     }
    //
    //     // add file to tracking slice
    //     testFiles = append(testFiles, absPath)
    // }

    // run the mixed read/write test
    fmt.Fprintf(os.Stderr, "starting mixed R/W test with %d workers (%d%% reads)\n", *parallelJobs, *rwmix)

    // get test results
    result, err := iolyzer.MixedRWTest(
        testFiles,
        *blockSize,
        *rwmix,
        *directIO,
        *oSync,
        *fsyncFreq,
        time.Duration(*testDuration)*time.Second,
    )
    if err != nil {
        fmt.Fprintf(os.Stderr, "test failed: %v\n", err)
        os.Exit(1)
    }

    // format and output the results
    output, err := iolyzer.FormatResult(result, format)
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to format results: %v\n", err)
        os.Exit(1)
    }

    // print the formatted output
    fmt.Print(output)
}

// import (
//     "fmt"
//     "os"
//     "path/filepath"
//     "time"
//
//     "github.com/jessegalley/iolyzer"
//     "github.com/spf13/pflag"
// )
//
// func main() {
//     var (
//         // size of test files in megabytes
//         fileSize = pflag.Int64("size", 1024, "size of each test file in megabytes")
//         
//         // base name for test files
//         fileName = pflag.String("file", "iolyzer_test", "base name for test files")
//         
//         // block size for io operations in kilobytes
//         blockSize = pflag.Int("block", 4096, "block size for io operations in bytes")
//         
//         // duration of test in seconds
//         testDuration = pflag.Int("runtime", 10, "duration of test in seconds")
//         
//         // number of parallel jobs
//         parallelJobs = pflag.IntP("parallel-jobs", "P", 1, "number of parallel jobs")
//         
//         // read/write mix percentage
//         rwmix = pflag.Int("rwmix", 50, "percentage of operations that should be reads (0-100)")
//         
//         // whether to use direct io
//         directIO = pflag.Bool("direct", false, "use direct io (o_direct)")
//         
//         // whether to use O_SYNC
//         oSync = pflag.Bool("osync", false, "use O_SYNC for writes")
//         
//         // fsync frequency
//         fsyncFreq = pflag.Int("fsync", 0, "call fsync after this many writes (0 disables)")
//     )
//
//     // parse command line flags
//     pflag.Parse()
//
//     // validate parameters
//     if *rwmix < 0 || *rwmix > 100 {
//         fmt.Printf("rwmix must be between 0 and 100, got %d\n", *rwmix)
//         os.Exit(1)
//     }
//
//     if *parallelJobs < 1 {
//         fmt.Printf("parallel-jobs must be at least 1, got %d\n", *parallelJobs)
//         os.Exit(1)
//     }
//
//     if *blockSize <= 0 {
//         fmt.Printf("block size must be positive, got %d\n", *blockSize)
//         os.Exit(1)
//     }
//
//     if *fileSize <= 0 {
//         fmt.Printf("file size must be positive, got %d\n", *fileSize)
//         os.Exit(1)
//     }
//
//     // create slice to track test files
//     var testFiles []string
//
//     // create test files for each worker
//     for i := 0; i < *parallelJobs; i++ {
//         // generate unique file name for this worker
//         workerFile := fmt.Sprintf("%s_%d.dat", *fileName, i)
//         
//         // ensure file path is absolute
//         absPath, err := filepath.Abs(workerFile)
//         if err != nil {
//             fmt.Printf("failed to get absolute path: %v\n", err)
//             os.Exit(1)
//         }
//
//         // create the test file
//         err = iolyzer.LayoutTestFile(absPath, int(*fileSize*1024*1024))
//         if err != nil {
//             fmt.Printf("failed to create test file %s: %v\n", workerFile, err)
//             // cleanup(testFiles)
//             os.Exit(1)
//         }
//
//         // add file to tracking slice
//         testFiles = append(testFiles, absPath)
//     }
//
//     // run the mixed read/write test
//     fmt.Printf("starting mixed R/W test with %d workers (%d%% reads)\n", *parallelJobs, *rwmix)
//
//     // get test results
//     result, err := iolyzer.MixedRWTest(
//       testFiles,
//       *blockSize,
//       *rwmix,
//       *directIO,
//       *oSync,
//       *fsyncFreq,
//       time.Duration(*testDuration)*time.Second,
//     )
//     if err != nil {
//       fmt.Printf("test failed: %v\n", err)
//       os.Exit(1)
//     }
//
//     // calculate metrics from test result
//     readIOPS := float64(result.ReadCount) / result.Duration.Seconds()
//     writeIOPS := float64(result.WriteCount) / result.Duration.Seconds()
//     readThroughput := float64(result.BytesRead) / result.Duration.Seconds() / (1024 * 1024)
//     writeThroughput := float64(result.BytesWritten) / result.Duration.Seconds() / (1024 * 1024)
//
//     // print results in table format
//     fmt.Printf("\n%8s  %12s  %12s\n", "", "IOPS", "BW (MB/s)")
//     fmt.Printf("%8s  %12.2f  %12.2f\n", "read", readIOPS, readThroughput)
//     fmt.Printf("%8s  %12.2f  %12.2f\n", "write", writeIOPS, writeThroughput)
//   }
//
// // actually would rather keep files for repeated tests I think
// // cleanup removes all test files
// // func cleanup(files []string) {
// //     // iterate through files
// //     for _, file := range files {
// //         // attempt to remove each file
// //         if err := os.Remove(file); err != nil {
// //             fmt.Printf("warning: failed to remove test file %s: %v\n", file, err)
// //         }
// //     }
// // }
