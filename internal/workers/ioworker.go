// package ioworker provides io operation worker functionality
package ioworker

import (
	"crypto/rand"
	"fmt"
	"io"
	"os"
	"syscall"
	"time"
	"unsafe"
)

// OP represents an io operation to be performed
type OP struct {
	OpType   string // type of operation to perform (read/write)
	OpRepeat int    // number of times to repeat this operation
}

// IOWorker performs io operations based on received commands
type IOWorker struct {
	Number     int                      // worker identifier
	BlockSize  int                      // size of io operations in bytes
	DirectIO   bool                     // whether to use direct io
	OSync      bool                     // whether to use O_SYNC
	FsyncFreq  int                      // frequency of fsync calls (0 disables)
	OpQueue    <-chan OP                // channel for receiving operations
	Results    chan<- map[string]uint64 // channel for sending performance results
	ErrorsCh   chan<- error             // channel for reporting errors
	done       chan struct{}            // internal shutdown signal
	WorkingDir string                   // directory to operate in
	file       *os.File                 // current open file handle
	readBuf    []byte                   // aligned buffer for read operations
	writeBuf   []byte                   // aligned buffer for write operations
}

// NewIOWorker creates a new IOWorker instance
func NewIOWorker(num int, wd string, blockSize int, direct, osync bool, fsyncFreq int) *IOWorker {
	// create worker instance with provided configuration
	w := &IOWorker{
		Number:     num,
		WorkingDir: wd,
		BlockSize:  blockSize,
		DirectIO:   direct,
		OSync:      osync,
		FsyncFreq:  fsyncFreq,
		done:       make(chan struct{}),
	}

	// initialize io buffers
	w.initBuffers()

	return w
}

// initBuffers creates and aligns buffers for io operations
func (w *IOWorker) initBuffers() {
	// set alignment requirement for direct io
	const alignment = 4096

	// create and align buffers based on direct io setting
	if w.DirectIO {
		// create oversized buffers to allow for alignment
		rawReadBuf := make([]byte, w.BlockSize+alignment*2)
		rawWriteBuf := make([]byte, w.BlockSize+alignment*2)

		// get aligned slices
		w.readBuf = alignBuffer(rawReadBuf, alignment)[:w.BlockSize]
		w.writeBuf = alignBuffer(rawWriteBuf, alignment)[:w.BlockSize]
	} else {
		// create simple buffers for normal io
		w.readBuf = make([]byte, w.BlockSize)
		w.writeBuf = make([]byte, w.BlockSize)
	}

	// fill write buffer with random data
	rand.Read(w.writeBuf)
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

// Start begins processing operations from the queue
func (w *IOWorker) Start() error {
	// open the target file
	if err := w.openFile(); err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	// launch worker goroutine
	go w.processOps()

	return nil
}

// Stop signals the worker to shut down
func (w *IOWorker) Stop() {
	// signal worker to stop

  fmt.Printf("DEBUG: worker: %d recieved Stop()\n", w.Number)
  
	close(w.done)

}

// openFile opens the worker's target file
func (w *IOWorker) openFile() error {
	// prepare open flags based on settings
	flags := os.O_RDWR
	if w.DirectIO {
		flags |= syscall.O_DIRECT
	}
	if w.OSync {
		flags |= syscall.O_SYNC
	}

	// construct file path
	path := fmt.Sprintf("%s/iolyzer_test_%d.dat", w.WorkingDir, w.Number)

	// open the file
	var err error
	w.file, err = os.OpenFile(path, flags, 0644)
	if err != nil {
		return fmt.Errorf("failed to open file: %w", err)
	}

	return nil
}

// processOps handles the main operation processing loop
func (w *IOWorker) processOps() {
	// initialize counters
	counters := make(map[string]uint64)
	counters["reads"] = 0
	counters["writes"] = 0
	counters["bytes_read"] = 0
	counters["bytes_written"] = 0

	// create ticker for periodic counter updates
	ticker := time.NewTicker(time.Second)
	defer ticker.Stop()

	// track write count for fsync
	writeCount := uint64(0)

	for {
		select {
		case <-w.done:
			// worker shutdown requested
      // cleanup resources
      if w.file != nil {
        w.file.Close()
      }

			return

		case op := <-w.OpQueue:
      // process the operation
      switch op.OpType {
      case "read":
        // perform read operation
        for i := 0; i < op.OpRepeat; i++ {
          n, err := io.ReadFull(w.file, w.readBuf)
          if err != nil && err != io.EOF && err != io.ErrUnexpectedEOF {
            w.ErrorsCh <- fmt.Errorf("read error: %w", err)
            continue
          }

        // update counters
        counters["reads"]++
        counters["bytes_read"] += uint64(n)
        }   
        
      case "write":
        // perform write operation
        fmt.Printf("DEBUG: worker: %d write\n", w.Number)
        n, err := w.file.Write(w.writeBuf)
        if err != nil {
          w.ErrorsCh <- fmt.Errorf("write error: %w", err)
          continue
        }

        // update counters
        counters["writes"]++
        counters["bytes_written"] += uint64(n)

        // handle fsync if enabled
        writeCount++
        if w.FsyncFreq > 0 && writeCount%uint64(w.FsyncFreq) == 0 {
          if err := w.file.Sync(); err != nil {
            w.ErrorsCh <- fmt.Errorf("fsync error: %w", err)
          }
        }
      }
    default:
      fmt.Printf("DEBUG: worker: %d found empty op queue\n", w.Number)
      // don't want to spam stdout, so sleep briefly
      time.Sleep(time.Millisecond)   

		case <-ticker.C:
			// send counter updates
			// w.Results <- counters
      select {
      case w.Results <- counters:
        // sent successfully
      default:
        fmt.Printf("DEBUG: worker: %d blocked sending results\n", w.Number)
      }

      // fmt.Printf("DEBUG: worker: %d noop\n", w.Number)
			// reset counters
			counters = make(map[string]uint64)
			counters["reads"] = 0
			counters["writes"] = 0
			counters["bytes_read"] = 0
			counters["bytes_written"] = 0
		}
	}
}

// package ioworker
//
// import "os"
//
// type OP struct {
// 	OpType   string // operation type (read/write)
// 	OpRepeat int    // number of times to repeat operation
// }
//
// type IOWorker struct {
// 	Number     int           // worker identifier
// 	BlockSize  int           // size of io operations
// 	DirectIO   bool          // use O_DIRECT
// 	OSync      bool          // use O_SYNC
// 	FsyncFreq  int           // fsync frequency
// 	OpQueue    chan<- OP     // operation queue
// 	Results    chan<- uint64 // raw counter updates
// 	ErrorsCh   chan<- error  // error reporting
// 	done       chan struct{} // internal shutdown signal
// 	workingDir string        // directory to operate in
// 	file       *os.File     // current open file
// }
//
// // worker should be given a starting dentry when
// // instantiated.  then it will dynamically decide
// // which file to open and start operating on based
// // on what is in the dentry
// func NewIOWorker(num int, wd string) *IOWorker {
// 	return &IOWorker{
// 		Number:     num,
// 		workingDir: wd,
// 	}
// }
//
// func (i *IOWorker) Start() {
// 	// starts the main worker goroutine
// 	// look up entries in current WorkingDir
// 	// decide which file to open based on Number
// 	// Open file, start processing OPs
// }
//
// func (i *IOWorker) Stop() {
// 	// sends the signal to stop the IOWorker
// 	// which tells the spawned worker goroutine to
// 	// stop, sync, close any open files, and close
// 	// results channel(s)
// }
//
// func (i *IOWorker) openFile(path string) {
// 	i.WorkingFile = path
// 	// open a file to begin doing OPs on it
// }
//
// func (i *IOWorker) closeFile() {
// 	// close the currently opened file
// }
//
// func (i *IOWorker) listCwd() []string {
// 	// get a listing of the current working dir
// 	// this listing wil be used to decide on what
// 	// to open next, based on what OP we're looking
// 	// to process
// 	var listing []string
// 	return listing
// }
//
// func (i *IOWorker) create() string {
// 	// attempts to create a file in the current working
// 	// directory and returns it's path
// 	var newPath string
// 	return newPath
// }
//
// func (i *IOWorker) touch(path string) error {
// 	// attempts to touch a given file or dir
// 	path = ""
// 	return nil
// }
//
// func (i *IOWorker) stat(path string) error {
// 	// attempts to stat a given file or dir
// 	path = ""
// 	return nil
// }
