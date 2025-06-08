// package runners contains io testing functionality
// common to all tests
package runners

import (
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
