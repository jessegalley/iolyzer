package iolyzer 

import (
    "crypto/rand"
    "fmt"
    "io"
    "os"
    "syscall"
    "time"
    "unsafe"
)

// Read performs reads on a file with optional direct io
// file: path to the file to read
// block: size of each read operation in bytes
// duration: how long to run the test
// direct: whether to use direct io (o_direct)
func Read(file string, block int, duration time.Duration, direct bool) error {
    // when using direct io, the buffer must be aligned to the block size of the filesystem
    const alignment = 4096

    // check alignment requirements for direct io
    if direct {
        if block%alignment != 0 {
            return fmt.Errorf("block size must be a multiple of %d bytes for direct io", alignment)
        }
    }

    // prepare open flags based on direct io setting
    flags := os.O_RDONLY
    if direct {
        flags |= syscall.O_DIRECT
    }

    // open file with appropriate flags
    f, err := os.OpenFile(file, flags, 0)
    if err != nil {
        return fmt.Errorf("failed to open file: %w", err)
    }
    defer f.Close()

    // create read buffer
    var readBuf []byte
    var alignedBuf []byte

    if direct {
        // for direct io, create an aligned buffer
        readBuf = make([]byte, block+alignment)
        offset := int(alignment - (uintptr(unsafe.Pointer(&readBuf[0]))&(alignment-1)))
        alignedBuf = readBuf[offset : offset+block]
    } else {
        // for normal io, create a simple buffer
        readBuf = make([]byte, block)
        alignedBuf = readBuf
    }

    // initialize counters
    var readCount int64
    var bytesRead int64

    // record start time
    start := time.Now()

    // read until duration is up
    for time.Since(start) < duration {
        // seek back to start of file if we've reached the end
        if _, err := f.Seek(0, 0); err != nil {
            return fmt.Errorf("failed to seek to start of file: %w", err)
        }

        // read a block of data
        n, err := io.ReadFull(f, alignedBuf)
        if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
            return fmt.Errorf("failed to read from file: %w", err)
        }

        // increment counters
        readCount++
        bytesRead += int64(n)
    }

    // calculate metrics
    elapsed := time.Since(start)
    iops := float64(readCount) / elapsed.Seconds()
    throughput := float64(bytesRead) / elapsed.Seconds()
    throughputMB := throughput / (1024 * 1024)

    // print results with io mode indicated
    ioMode := "Normal"
    if direct {
        ioMode = "Direct"
    }
    fmt.Printf("%s Read IOPS: %.2f\n", ioMode, iops)
    fmt.Printf("%s Read throughput: %.2f MB/s\n", ioMode, throughputMB)

    return nil
}

// StatTest performs continuous stat operations on a file for a given duration
// file: path to the file to stat
// duration: how long to run the test
func StatTest(file string, duration time.Duration) error {
    // prepare stat buffer for reuse
    var stat syscall.Stat_t
    
    // initialize counter
    var statCount int64
    
    // record start time
    start := time.Now()
    
    // stat until duration is up
    for time.Since(start) < duration {
        // perform stat operation
        err := syscall.Stat(file, &stat)
        if err != nil {
            return fmt.Errorf("failed to stat file: %w", err)
        }
        
        // increment counter
        statCount++
    }
    
    // calculate actual test duration
    elapsed := time.Since(start)
    
    // calculate operations per second
    opsPerSec := float64(statCount) / elapsed.Seconds()
    
    // print results
    fmt.Printf("Stat operations per second: %.2f\n", opsPerSec)
    
    return nil
}

// LayoutTestFile creates a file of specified size filled with random data
// file: path to the file to create
// size: size of the file in bytes
func LayoutTestFile(file string, size int) error {
    // create a buffer for random data
    randomData := make([]byte, size)

    // fill buffer with random data
    _, err := rand.Read(randomData)
    if err != nil {
        return fmt.Errorf("failed to generate random data: %w", err)
    }

    // create the test file
    f, err := os.OpenFile(file, os.O_CREATE|os.O_WRONLY, 0644)
    if err != nil {
        return fmt.Errorf("failed to create file: %w", err)
    }

    // ensure file is closed when function returns
    defer f.Close()

    // write the random data to the file
    _, err = f.Write(randomData)
    if err != nil {
        return fmt.Errorf("failed to write random data to file: %w", err)
    }

    // sync file to ensure data is written to disk
    err = f.Sync()
    if err != nil {
        return fmt.Errorf("failed to sync file: %w", err)
    }

    return nil
}

// // ReadDirect performs direct reads on a file, bypassing the page cache
// // file: path to the file to read
// // block: size of each read operation in bytes
// // duration: how long to run the test
// func ReadDirect(file string, block int, duration time.Duration) error {
//     // when using o_direct, the buffer must be aligned to the block size of the filesystem
//     const alignment = 4096
//
//     // ensure block size is aligned
//     if block%alignment != 0 {
//         return fmt.Errorf("block size must be a multiple of %d bytes", alignment)
//     }
//
//     // open file for reading with o_direct flag
//     f, err := os.OpenFile(file, os.O_RDONLY|syscall.O_DIRECT, 0)
//     if err != nil {
//         return fmt.Errorf("failed to open file for direct reading: %w", err)
//     }
//     defer f.Close()
//
//     // create an aligned buffer for reading blocks
//     readBuf := make([]byte, block+alignment)
//
//     // ensure the slice is aligned to the required boundary
//     offset := int(alignment - (uintptr(unsafe.Pointer(&readBuf[0]))&(alignment-1)))
//     alignedBuf := readBuf[offset : offset+block]
//
//     // initialize counters
//     var readCount int64
//     var bytesRead int64
//
//     // record start time
//     start := time.Now()
//
//     // read until duration is up
//     for time.Since(start) < duration {
//         // seek back to start of file if we've reached the end
//         if _, err := f.Seek(0, 0); err != nil {
//             return fmt.Errorf("failed to seek to start of file: %w", err)
//         }
//
//         // read a block of data
//         n, err := io.ReadFull(f, alignedBuf)
//         if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
//             return fmt.Errorf("failed to read from file: %w", err)
//         }
//
//         // increment counters
//         readCount++
//         bytesRead += int64(n)
//     }
//
//     // calculate metrics
//     elapsed := time.Since(start)
//     iops := float64(readCount) / elapsed.Seconds()
//     throughput := float64(bytesRead) / elapsed.Seconds()
//     throughputMB := throughput / (1024 * 1024)
//
//     // print results
//     fmt.Printf("Direct Read IOPS: %.2f\n", iops)
//     fmt.Printf("Direct Read throughput: %.2f MB/s\n", throughputMB)
//
//     return nil
// }
//
// // ReadNormal performs normal buffered reads on a file
// // file: path to the file to read
// // block: size of each read operation in bytes
// // duration: how long to run the test
// func ReadNormal(file string, block int, duration time.Duration) error {
//     // open file for normal reading
//     f, err := os.Open(file)
//     if err != nil {
//         return fmt.Errorf("failed to open file for reading: %w", err)
//     }
//     defer f.Close()
//
//     // create a buffer for reading blocks
//     readBuf := make([]byte, block)
//
//     // initialize counters
//     var readCount int64
//     var bytesRead int64
//
//     // record start time
//     start := time.Now()
//
//     // read until duration is up
//     for time.Since(start) < duration {
//         // seek back to start of file if we've reached the end
//         if _, err := f.Seek(0, 0); err != nil {
//             return fmt.Errorf("failed to seek to start of file: %w", err)
//         }
//
//         // read a block of data
//         n, err := io.ReadFull(f, readBuf)
//         if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
//             return fmt.Errorf("failed to read from file: %w", err)
//         }
//
//         // increment counters
//         readCount++
//         bytesRead += int64(n)
//     }
//
//     // calculate metrics
//     elapsed := time.Since(start)
//     iops := float64(readCount) / elapsed.Seconds()
//     throughput := float64(bytesRead) / elapsed.Seconds()
//     throughputMB := throughput / (1024 * 1024)
//
//     // print results
//     fmt.Printf("Normal Read IOPS: %.2f\n", iops)
//     fmt.Printf("Normal Read throughput: %.2f MB/s\n", throughputMB)
//
//     return nil
// }


