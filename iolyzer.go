// package iolyzer contains io testing functionality
package iolyzer

import (
    "crypto/rand"
    "fmt"
    "io"
    mathrand "math/rand"
    "os"
    "runtime"
    "sync"
    "syscall"
    "time"
    "unsafe"
)

// TestResult contains the metrics from a single worker's test run
type TestResult struct {
    // number of read operations completed
    ReadCount int64
    
    // number of write operations completed
    WriteCount int64
    
    // total bytes read
    BytesRead int64
    
    // total bytes written
    BytesWritten int64
    
    // duration of the test
    Duration time.Duration
}

// WorkerConfig contains the configuration for a single worker
type WorkerConfig struct {
    // path to the test file
    FilePath string
    
    // size of each io operation in bytes
    BlockSize int
    
    // percentage of operations that should be reads (0-100)
    ReadPercentage int
    
    // whether to use direct io
    DirectIO bool
    
    // whether to use O_SYNC
    OSync bool
    
    // frequency of fsync calls (0 disables)
    FsyncFrequency int
    
    // duration to run the test
    Duration time.Duration
    
    // channel to send results back to main thread
    Results chan<- TestResult
}

// // MixedRWTest performs parallel mixed read/write testing
// func MixedRWTest(files []string, block int, rwmix int, direct bool, osync bool, fsyncFreq int, duration time.Duration) error {
//     // constant for alignment requirement in direct io
//     const alignment = 4096
//
//     // validate block size for direct io
//     if direct && block%alignment != 0 {
//         return fmt.Errorf("block size must be a multiple of %d bytes for direct io", alignment)
//     }
//
//     // create buffered channel for results
//     results := make(chan TestResult, len(files))
//
//     // create wait group for synchronization
//     var wg sync.WaitGroup
//
//     // set GOMAXPROCS to number of workers for better CPU utilization
//     runtime.GOMAXPROCS(len(files))
//
//     // launch worker goroutines
//     for i, file := range files {
//         // increment wait group counter
//         wg.Add(1)
//
//         // create worker config
//         cfg := WorkerConfig{
//             FilePath:       file,
//             BlockSize:      block,
//             ReadPercentage: rwmix,
//             DirectIO:       direct,
//             OSync:         osync,
//             FsyncFrequency: fsyncFreq,
//             Duration:       duration,
//             Results:       results,
//         }
//
//         // create worker-specific RNG with unique seed
//         rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(i)))
//
//         // launch worker goroutine
//         go func(config WorkerConfig, workerRng *mathrand.Rand) {
//             // ensure wait group is decremented when worker completes
//             defer wg.Done()
//
//             // lock this goroutine to its thread for better performance
//             runtime.LockOSThread()
//             
//             // run the worker
//             if err := worker(config, workerRng); err != nil {
//                 fmt.Printf("worker error on file %s: %v\n", config.FilePath, err)
//             }
//         }(cfg, rng)
//     }
//
//     // wait for all workers to complete
//     wg.Wait()
//
//     // close results channel
//     close(results)
//
//     // aggregate results
//     var totalReads, totalWrites int64
//     var totalBytesRead, totalBytesWritten int64
//     var maxDuration time.Duration
//
//     // collect results from channel
//     for result := range results {
//         // accumulate counters
//         totalReads += result.ReadCount
//         totalWrites += result.WriteCount
//         totalBytesRead += result.BytesRead
//         totalBytesWritten += result.BytesWritten
//         
//         // track longest duration
//         if result.Duration > maxDuration {
//             maxDuration = result.Duration
//         }
//     }
//
//     // calculate aggregate metrics
//     readIOPS := float64(totalReads) / maxDuration.Seconds()
//     writeIOPS := float64(totalWrites) / maxDuration.Seconds()
//     readThroughput := float64(totalBytesRead) / maxDuration.Seconds() / (1024 * 1024)
//     writeThroughput := float64(totalBytesWritten) / maxDuration.Seconds() / (1024 * 1024)
//
//     // print results
//     // fmt.Printf("read IOPS: %.2f\n", readIOPS)
//     // fmt.Printf("write IOPS: %.2f\n", writeIOPS)
//     // fmt.Printf("wead throughput: %.2f MB/s\n", readThroughput)
//     // fmt.Printf("write throughput: %.2f MB/s\n", writeThroughput)
// 		// // fmt.Printf("Test duration: %.2f seconds\n", maxDuration.Seconds())
// 		// print results in table format
// 		fmt.Printf("\n%8s  %12s  %12s\n", "", "IOPS", "BW (MB/s)")
// 		fmt.Printf("%8s  %12.2f  %12.2f\n", "read", readIOPS, readThroughput)
// 		fmt.Printf("%8s  %12.2f  %12.2f\n", "write", writeIOPS, writeThroughput)
//
//     return nil
// }
// MixedRWTest performs parallel mixed read/write testing and returns aggregated results
func MixedRWTest(files []string, block int, rwmix int, direct bool, osync bool, fsyncFreq int, duration time.Duration) (TestResult, error) {
    // constant for alignment requirement in direct io
    const alignment = 4096

    // validate block size for direct io
    if direct && block%alignment != 0 {
        return TestResult{}, fmt.Errorf("block size must be a multiple of %d bytes for direct io", alignment)
    }

    // create buffered channel for results
    results := make(chan TestResult, len(files))

    // create wait group for synchronization
    var wg sync.WaitGroup

    // set GOMAXPROCS to number of workers for better CPU utilization
    runtime.GOMAXPROCS(len(files))

    // launch worker goroutines
    for i, file := range files {
        // increment wait group counter
        wg.Add(1)

        // create worker config
        cfg := WorkerConfig{
            FilePath:       file,
            BlockSize:      block,
            ReadPercentage: rwmix,
            DirectIO:       direct,
            OSync:         osync,
            FsyncFrequency: fsyncFreq,
            Duration:       duration,
            Results:       results,
        }

        // create worker-specific RNG with unique seed
        rng := mathrand.New(mathrand.NewSource(time.Now().UnixNano() + int64(i)))

        // launch worker goroutine
        go func(config WorkerConfig, workerRng *mathrand.Rand) {
            // ensure wait group is decremented when worker completes
            defer wg.Done()

            // lock this goroutine to its thread for better performance
            runtime.LockOSThread()
            
            // run the worker
            if err := worker(config, workerRng); err != nil {
                fmt.Printf("worker error on file %s: %v\n", config.FilePath, err)
            }
        }(cfg, rng)
    }

    // wait for all workers to complete
    wg.Wait()

    // close results channel
    close(results)

    // create aggregate result struct
    aggregateResult := TestResult{}

    // collect results from channel
    for result := range results {
        // accumulate counters into aggregate result
        aggregateResult.ReadCount += result.ReadCount
        aggregateResult.WriteCount += result.WriteCount
        aggregateResult.BytesRead += result.BytesRead
        aggregateResult.BytesWritten += result.BytesWritten
        
        // track longest duration
        if result.Duration > aggregateResult.Duration {
            aggregateResult.Duration = result.Duration
        }
    }

    return aggregateResult, nil
}

// alignBuffer ensures a byte slice is aligned to the given boundary
func alignBuffer(buf []byte, alignment int) []byte {
    // calculate offset needed for alignment
    addr := uintptr(unsafe.Pointer(&buf[0]))
    alignmentUptr := uintptr(alignment)
    offset := int(alignmentUptr - (addr & (alignmentUptr - 1)))
    
    // return aligned slice
    if offset == alignment {
        return buf
    }
    return buf[offset:]
}

// worker performs the actual io operations for a single test file
func worker(cfg WorkerConfig, rng *mathrand.Rand) error {
    // constant for alignment requirement in direct io
    const alignment = 4096

    // prepare open flags based on settings
    flags := os.O_RDWR
    if cfg.DirectIO {
        flags |= syscall.O_DIRECT
    }
    if cfg.OSync {
        flags |= syscall.O_SYNC
    }

    // open the test file
    f, err := os.OpenFile(cfg.FilePath, flags, 0)
    if err != nil {
        return fmt.Errorf("failed to open file: %w", err)
    }
    
    // ensure file is closed when function returns
    defer f.Close()

    // get file size once at start
    fileInfo, err := f.Stat()
    if err != nil {
        return fmt.Errorf("failed to stat file: %w", err)
    }
    fileSize := fileInfo.Size()

    // calculate number of blocks in file
    numBlocks := fileSize / int64(cfg.BlockSize)
    if numBlocks <= 0 {
        return fmt.Errorf("file size must be larger than block size")
    }

    // create buffers for io operations
    var readBuf, writeBuf []byte
    var alignedReadBuf, alignedWriteBuf []byte

    // initialize buffers based on direct io setting
    if cfg.DirectIO {
        // create oversized buffers to allow for alignment
        readBuf = make([]byte, cfg.BlockSize+alignment*2)
        writeBuf = make([]byte, cfg.BlockSize+alignment*2)
        
        // get aligned slices
        alignedReadBuf = alignBuffer(readBuf, alignment)[:cfg.BlockSize]
        alignedWriteBuf = alignBuffer(writeBuf, alignment)[:cfg.BlockSize]
        
        // fill write buffer with random data
        if _, err := rand.Read(alignedWriteBuf); err != nil {
            return fmt.Errorf("failed to generate random data: %w", err)
        }
    } else {
        // create simple buffers for normal io
        readBuf = make([]byte, cfg.BlockSize)
        writeBuf = make([]byte, cfg.BlockSize)
        alignedReadBuf = readBuf
        alignedWriteBuf = writeBuf
        
        // fill write buffer with random data
        if _, err := rand.Read(writeBuf); err != nil {
            return fmt.Errorf("failed to generate random data: %w", err)
        }
    }

    // initialize result counters
    result := TestResult{}

    // record start time
    start := time.Now()

    // perform io operations until duration is reached
    for time.Since(start) < cfg.Duration {
        // generate random number to determine operation type
        op := rng.Intn(100)
        
        // calculate random block-aligned offset
        blockNum := rng.Int63n(numBlocks)
        offset := blockNum * int64(cfg.BlockSize)
        
        // perform seek
        if _, err := f.Seek(offset, 0); err != nil {
            return fmt.Errorf("failed to seek: %w", err)
        }

        // perform read or write based on percentage
        if op < cfg.ReadPercentage {
            // perform read operation
            n, err := io.ReadFull(f, alignedReadBuf)
            if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
                return fmt.Errorf("failed to read: %w", err)
            }
            
            // update read counters
            result.ReadCount++
            result.BytesRead += int64(n)
        } else {
            // perform write operation with sync
            n, err := f.Write(alignedWriteBuf)
            if err != nil {
                return fmt.Errorf("failed to write: %w", err)
            }
            
            // update write counters
            result.WriteCount++
            result.BytesWritten += int64(n)
            
            // handle fsync if enabled
            if cfg.FsyncFrequency > 0 && result.WriteCount%int64(cfg.FsyncFrequency) == 0 {
                if err := f.Sync(); err != nil {
                    return fmt.Errorf("failed to fsync: %w", err)
                }
            }
        }
    }

    // record test duration
    result.Duration = time.Since(start)

    // send results back to main thread
    cfg.Results <- result

    return nil
}
