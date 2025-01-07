package main

import (
    "fmt"
    "os"
    "time"

    "github.com/jessegalley/iolyzer"
    "github.com/spf13/pflag"
)

func main() {
    // define command line flags
    rootDir := pflag.StringP("root", "r", "testfs", "root directory for test filesystem")
    numDirs := pflag.IntP("dirs", "d", 100, "number of directories to create")
    numFiles := pflag.IntP("files", "f", 100, "number of files to create")
    fileSize := pflag.Int64P("size", "s", 4096, "size of each file in bytes")
    duration := pflag.DurationP("time", "t", 10*time.Second, "duration to run the tests")
    
    // parse command line flags
    pflag.Parse()

    // create new layout configuration
    layout := iolyzer.NewLayout(*rootDir, *numDirs, *numFiles, *fileSize)

    // create the test filesystem
    fmt.Printf("Creating test filesystem with %d directories and %d files...\n", *numDirs, *numFiles)
    err := layout.Create()
    if err != nil {
        fmt.Printf("Failed to create test filesystem: %v\n", err)
        os.Exit(1)
    }

    // run stat test
    fmt.Println("Starting stat test...")
    err = iolyzer.StatTest(*rootDir, *duration)
    if err != nil {
        fmt.Printf("Stat test failed: %v\n", err)
        return
    }

    // perform direct reads on some files
    fmt.Println("\nStarting direct read test...")
    // note: we're using the first file in the first directory for this test
    testFile := fmt.Sprintf("%s/dir_l1_0/file_0", *rootDir)
    err = iolyzer.Read(testFile, 4096, *duration, true)
    if err != nil {
        fmt.Printf("Direct read test failed: %v\n", err)
        return
    }

    // cleanup unless user specifies otherwise
    // note: you might want to add a flag to control this
    fmt.Println("\nCleaning up test filesystem...")
    err = os.RemoveAll(*rootDir)
    if err != nil {
        fmt.Printf("Failed to clean up test filesystem: %v\n", err)
    }
}
// package main
//
// import (
// 	"fmt"
// 	"os"
// 	"time"
//
// 	"github.com/jessegalley/iolyzer"
// )
//
// func main() {
//     // example parameters
//     file := "test.data"
//     size := 1024 * 1024     // 1MB
//     block := 4096           // 4KB blocks
//     duration := time.Second * 10
//
//     // create the test file
//     err := iolyzer.LayoutTestFile(file, size)
//     if err != nil {
//         fmt.Printf("Failed to create test file: %v\n", err)
//         return
//     }
//
//     // run stat test
//     fmt.Println("Starting stat test...")
//     err = iolyzer.StatTest(file, duration)
//     if err != nil {
//         fmt.Printf("Stat test failed: %v\n", err)
//         return
//     }
//
//     // perform direct reads
//     fmt.Println("\nStarting direct read test...")
//     err = iolyzer.Read(file, block, duration, true)
//     if err != nil {
//         fmt.Printf("direct read test failed: %v\n", err)
//         return
//     }
//
//     // fmt.Println("Starting direct read test...")
//     // err = iolyzer.ReadDirect(file, block, duration)
//     // if err != nil {
//     //     fmt.Printf("Direct read test failed: %v\n", err)
//     //     return
//     // }
//
//     // perform normal reads
//     // fmt.Println("\nStarting normal read test...")
//     // err = iolyzer.Read(file, block, duration, false)
//     // if err != nil {
//     //     fmt.Printf("Normal read test failed: %v\n", err)
//     //     return
//     // }
//
//    
//     // cleanup
//     os.Remove(file)
// }
//
