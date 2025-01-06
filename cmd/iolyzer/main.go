package main

import (
	"fmt"
	"os"
	"time"

	"github.com/jessegalley/iolyzer"
)

func main() {
    // example parameters
    file := "test.data"
    size := 1024 * 1024     // 1MB
    block := 4096           // 4KB blocks
    duration := time.Second * 30

    // create the test file
    err := iolyzer.LayoutTestFile(file, size)
    if err != nil {
        fmt.Printf("Failed to create test file: %v\n", err)
        return
    }

    // perform direct reads
    fmt.Println("Starting direct read test...")
    err = iolyzer.ReadDirect(file, block, duration)
    if err != nil {
        fmt.Printf("Direct read test failed: %v\n", err)
        return
    }

    // perform normal reads
    fmt.Println("\nStarting normal read test...")
    err = iolyzer.ReadNormal(file, block, duration)
    if err != nil {
        fmt.Printf("Normal read test failed: %v\n", err)
        return
    }

    // cleanup
    os.Remove(file)
}

