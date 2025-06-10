package layout

import (
	"fmt"
	"os"
	"path/filepath"
	"syscall"
)

// CreateMDSThrashDirectories creates the required directory structure for the test
// ensures that the tree <testdir>/{in,out}/{01..NN} exists and is accessible
// this function only creates directories and does not perform write tests
func CreateMDSThrashDirectories(testDir string, dirCount int) error {
	// create base directories for input and output
	inDir := filepath.Join(testDir, "in")
	outDir := filepath.Join(testDir, "out")

	// create input and output base directories
	err := os.MkdirAll(inDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create input directory: %w", err)
	}

	err = os.MkdirAll(outDir, 0755)
	if err != nil {
		return fmt.Errorf("failed to create output directory: %w", err)
	}

	// create numbered subdirectories in both input and output
	for i := 1; i <= dirCount; i++ {
		// create input subdirectory
		inSubDir := filepath.Join(inDir, fmt.Sprintf("%02d", i))
		err = os.MkdirAll(inSubDir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create input subdirectory %s: %w", inSubDir, err)
		}

		// create output subdirectory
		outSubDir := filepath.Join(outDir, fmt.Sprintf("%02d", i))
		err = os.MkdirAll(outSubDir, 0755)
		if err != nil {
			return fmt.Errorf("failed to create output subdirectory %s: %w", outSubDir, err)
		}
	}

	return nil
}

// ValidateMDSThrashDirectories validates that the required directory structure exists
// and appears to be writable based on permissions. this function is safe for concurrent
// use across multiple hosts as it only reads directory information without creating files
func ValidateMDSThrashDirectories(testDir string, dirCount int) error {
	// validate base directories for input and output
	inDir := filepath.Join(testDir, "in")
	outDir := filepath.Join(testDir, "out")

	// check that input base directory exists and is writable
	err := validateDirectoryWritable(inDir)
	if err != nil {
		return fmt.Errorf("input directory validation failed: %w", err)
	}

	// check that output base directory exists and is writable
	err = validateDirectoryWritable(outDir)
	if err != nil {
		return fmt.Errorf("output directory validation failed: %w", err)
	}

	// validate numbered subdirectories in both input and output
	for i := 1; i <= dirCount; i++ {
		// validate input subdirectory
		inSubDir := filepath.Join(inDir, fmt.Sprintf("%02d", i))
		err = validateDirectoryWritable(inSubDir)
		if err != nil {
			return fmt.Errorf("input subdirectory %s validation failed: %w", inSubDir, err)
		}

		// validate output subdirectory
		outSubDir := filepath.Join(outDir, fmt.Sprintf("%02d", i))
		err = validateDirectoryWritable(outSubDir)
		if err != nil {
			return fmt.Errorf("output subdirectory %s validation failed: %w", outSubDir, err)
		}
	}

	return nil
}

// validateDirectoryWritable checks if a directory exists and appears writable based on permissions
// this function uses filesystem permission checks rather than attempting actual file creation
// to avoid race conditions when multiple processes are validating concurrently
func validateDirectoryWritable(dirPath string) error {
	// get directory information
	info, err := os.Stat(dirPath)
	if err != nil {
		if os.IsNotExist(err) {
			return fmt.Errorf("directory %s does not exist", dirPath)
		}
		return fmt.Errorf("failed to stat directory %s: %w", dirPath, err)
	}

	// verify path is actually a directory
	if !info.IsDir() {
		return fmt.Errorf("%s exists but is not a directory", dirPath)
	}

	// check if directory appears writable based on permissions
	// get the current user's uid and gid for permission checking
	currentUID := os.Getuid()
	currentGID := os.Getgid()

	// get the directory's permission bits and ownership
	mode := info.Mode()
	stat := info.Sys().(*syscall.Stat_t)
	fileUID := int(stat.Uid)
	fileGID := int(stat.Gid)

	// check write permissions based on ownership
	var hasWritePermission bool

	if currentUID == fileUID {
		// user owns the directory, check user write permission
		hasWritePermission = mode&0200 != 0
	} else if currentGID == fileGID {
		// user is in the same group as directory, check group write permission
		hasWritePermission = mode&0020 != 0
	} else {
		// user is neither owner nor in group, check other write permission
		hasWritePermission = mode&0002 != 0
	}

	// return error if directory doesn't appear writable
	if !hasWritePermission {
		return fmt.Errorf("directory %s does not have write permissions for current user", dirPath)
	}

	return nil
}

// package layout
//
// import (
// 	"fmt"
// 	"os"
// 	"path/filepath"
// )
//
// // createMDSThrashDirectories creates the required directory structure for the test
// // ensures that the tree <testdir>/{in,out}/{01..NN} exists and is accessible
// func CreateMDSThrashDirectories(testDir string, dirCount int) error {
// 	// create base directories for input and output
// 	inDir := filepath.Join(testDir, "in")
// 	outDir := filepath.Join(testDir, "out")
//
// 	// create input and output base directories
// 	err := os.MkdirAll(inDir, 0755)
// 	if err != nil {
// 		return fmt.Errorf("failed to create input directory: %w", err)
// 	}
//
// 	err = os.MkdirAll(outDir, 0755)
// 	if err != nil {
// 		return fmt.Errorf("failed to create output directory: %w", err)
// 	}
//
// 	// create numbered subdirectories in both input and output
// 	for i := 1; i <= dirCount; i++ {
// 		// create input subdirectory
// 		inSubDir := filepath.Join(inDir, fmt.Sprintf("%02d", i))
// 		err = os.MkdirAll(inSubDir, 0755)
// 		if err != nil {
// 			return fmt.Errorf("failed to create input subdirectory %s: %w", inSubDir, err)
// 		}
//
// 		// create output subdirectory
// 		outSubDir := filepath.Join(outDir, fmt.Sprintf("%02d", i))
// 		err = os.MkdirAll(outSubDir, 0755)
// 		if err != nil {
// 			return fmt.Errorf("failed to create output subdirectory %s: %w", outSubDir, err)
// 		}
//
// 		// verify directories are writable by attempting to create a test file
// 		testFile := filepath.Join(inSubDir, ".write_test")
// 		f, err := os.Create(testFile)
// 		if err != nil {
// 			return fmt.Errorf("input directory %s is not writable: %w", inSubDir, err)
// 		}
// 		f.Close()
// 		os.Remove(testFile)
//
// 		testFile = filepath.Join(outSubDir, ".write_test")
// 		f, err = os.Create(testFile)
// 		if err != nil {
// 			return fmt.Errorf("output directory %s is not writable: %w", outSubDir, err)
// 		}
// 		f.Close()
// 		os.Remove(testFile)
// 	}
//
// 	return nil
// }
//
