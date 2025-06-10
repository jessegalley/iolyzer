/*
 *
 * jesse galley <jesse@jessegalley.net>
 */

// Package iotest abstracts the various test scenarios that
// can be run with iolyzer. Some tests have different options
// and different result sets
package iotest

import "time"

// Config holds all configuration parameters for iolyzer tests
type Config struct {
	TestDir      string        // test directory where test files will be created
	FileSize     int64         // size of each test file in bytes
	BlockSize    int           // size of each io operation in bytes
	ParallelJobs int           // number of parallel worker processes
	TestDuration time.Duration // duration to run the test
	DirectIO     bool          // whether to use direct io (o_direct)
	OSync        bool          // whether to use o_sync for writes
	FsyncFreq    int           // call fsync after this many writes (0 disables)
	OutFmt       string        // output format (table, json, or flat)
	ReInit       bool          // reinitialize test files even if they already exist
	Debug        int           // debug level (0=none, 1=basic, 2=verbose)

	// mixedrw specific test params
	FileName     string // base name for test files
	ReadWriteMix int    // percentage of operations that should be reads (0-100)

	// mds thrash specific test params
	DirCount       int // number of directories used in metadata stress test
	FilesPerBatch  int // number of files to write per test phase
	FilesPerMove   int // number of files to write before moving (0 no limit)
	FsyncCloseFreq int // fsync before closing this percentage of files
	MaxReadFiles   int // limit the number of files processed in the read/unlink phase (0 no limit)
}

// New creates a new Config instance with sensible default values
func NewConfig() *Config {
	return &Config{
		TestDir:        "./iolyzer_test/", // default test directory in current working directory
		FileSize:       10485760,          // 10 MiB
		BlockSize:      4096,              // default block size of 4k for most filesystems
		ParallelJobs:   1,                 // single worker by default
		TestDuration:   time.Second * 30,  // default test duration of 30 seconds
		DirectIO:       false,             // direct io disabled by default
		OSync:          false,             // o_sync disabled by default
		FsyncFreq:      0,                 // fsync disabled by default
		OutFmt:         "table",           // human readable table format by default
		ReInit:         false,             // don't reinitialize existing files by default
		Debug:          0,                 // no debug output by default
		FileName:       "test",            // default base filename
		ReadWriteMix:   80,                // default 75% reads, 25% writes
		DirCount:       16,                // default directory count for metadata tests
		FilesPerBatch:  100,               // default files per batch for metadata tests
		FilesPerMove:   0,                 // default files per move for metadata tests
		FsyncCloseFreq: 0,                 // default fsync frequency for metadata tests (10%)
		MaxReadFiles:   0,                 // default max read files for metadata tests
	}
}
