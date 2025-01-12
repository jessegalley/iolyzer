package main

import (
    "fmt"
    "os"
    "path/filepath"
    "time"

    "github.com/jessegalley/iolyzer"
    "github.com/spf13/pflag"
)

// program flags defined as global variables for access across functions
var (
    fileSize int64   // size of test files in megabytes
    fileName string  // base name for test files
    blockSize int    // block size for io operations in kilobytes
    testDuration int // duration of test in seconds
    parallelJobs int // number of parallel jobs
    rwmix int        // read/write mix percentage
    directIO bool    // whether to use direct io
    oSync bool       // whether to use O_SYNC
    fsyncFreq int    // fsync frequency
    outFmt string    // output format
    reinitFile bool  // whether to reinitialize existing test files
    version bool     // print version and exit
    metamix int      // metadata test
)

// init initializes and parses command line flags
func init() {
    // define command line flags, writing values to our global variables
    pflag.Int64VarP(&fileSize, "size", "s",  1024, "size of each test file in megabytes")
    pflag.StringVar(&fileName, "file", "iolyzer_test", "base name for test files")
    pflag.IntVarP(&blockSize, "block", "b", 4096, "block size for io operations in bytes")
    pflag.IntVarP(&testDuration, "runtime", "t",  10, "duration of test in seconds")
    pflag.IntVarP(&parallelJobs, "parallel-jobs", "P", 1, "number of parallel jobs")
    pflag.IntVar(&rwmix, "rwmix", 50, "percentage of operations that should be reads (0-100)")
    pflag.BoolVarP(&directIO, "direct", "d", false, "use direct io (o_direct)")
    pflag.BoolVar(&oSync, "osync", false, "use O_SYNC for writes")
    pflag.IntVar(&fsyncFreq, "fsync", 0, "call fsync after this many writes (0 disables)")
    pflag.StringVar(&outFmt, "format", "table", "output format (table, json, or flat)")
    pflag.BoolVar(&reinitFile, "reinit", false, "reinitialize test files even if they already exist")
    pflag.BoolVarP(&version, "version", "V", false, "print version and exit")
    pflag.IntVarP(&metamix, "metamix", "m", 0, "how much metadata ops to mix in (0 is no md ops)")

    // parse command line flags
    pflag.Parse()
}

func main() {
    // program version constant
    const progVersion string = "0.1.6"

    // check if version flag was set
    if version {
        fmt.Printf("iolyzer v%s\njesse@jessegalley.net\ngithub.com/jessegalley/iolyzer\n", progVersion)
        os.Exit(1)
    }

    // get the test directory from positional arguments
    testDir := getTestDirectory()

    // validate command line parameters
    if err := validateParameters(); err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    // validate output format
    format, err := iolyzer.ValidateFormat(outFmt)
    if err != nil {
        fmt.Fprintln(os.Stderr, err)
        os.Exit(1)
    }

    // handle test execution based on metamix setting
    if metamix < 1 {
        // perform simple read/write test
        if err := doSimpleTest(testDir, format); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    } else {
        // perform metadata-intensive test
        if err := doMetaTest(testDir, format); err != nil {
            fmt.Fprintln(os.Stderr, err)
            os.Exit(1)
        }
    }
}

// getTestDirectory determines and creates if necessary the test directory
func getTestDirectory() string {
    // check if a positional argument was provided
    if pflag.NArg() > 0 {
        // use the first positional argument as the test directory
        testDir := pflag.Arg(0)

        // ensure the directory exists
        if err := os.MkdirAll(testDir, 0755); err != nil {
            fmt.Fprintf(os.Stderr, "failed to create test directory %s: %v\n", testDir, err)
            os.Exit(1)
        }
        return testDir
    }

    // if no directory was specified, use current working directory
    testDir, err := os.Getwd()
    if err != nil {
        fmt.Fprintf(os.Stderr, "failed to get current working directory: %v\n", err)
        os.Exit(1)
    }
    return testDir
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

// doSimpleTest performs basic read/write testing without metadata operations
func doSimpleTest(testDir string, format iolyzer.OutputFormat) error {
    // create slice to track test files
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
            return fmt.Errorf("failed to get absolute path: %v", err)
        }

        // create the test file
        if err := iolyzer.LayoutTestFile(absPath, int(fileSize*1024*1024), reinitFile); err != nil {
            return fmt.Errorf("failed to create test file %s: %v", workerFile, err)
        }

        // add file to tracking slice
        testFiles = append(testFiles, absPath)
    }

    // announce test start
    fmt.Fprintf(os.Stderr, "starting mixed R/W test with %d workers (%d%% reads)\n", parallelJobs, rwmix)

    // execute the mixed read/write test
    result, err := iolyzer.MixedRWTest(
        testFiles,
        blockSize,
        rwmix,
        directIO,
        oSync,
        fsyncFreq,
        time.Duration(testDuration)*time.Second,
    )
    if err != nil {
        return fmt.Errorf("test failed: %v", err)
    }

    // format and output the results
    output, err := iolyzer.FormatResult(result, format)
    if err != nil {
        return fmt.Errorf("failed to format results: %v", err)
    }

    // print the formatted output
    fmt.Print(output)
    return nil
}

// doMetaTest performs testing with metadata operations mixed in
func doMetaTest(testDir string, format iolyzer.OutputFormat) error {
    // TODO: implement metadata testing logic
    return fmt.Errorf("metadata testing not yet implemented")
}
