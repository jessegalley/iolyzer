package layout 

import (
    "fmt"
    "math"
		"crypto/rand"
    "os"
    "path/filepath"
)

// Layout represents the configuration for creating a test filesystem layout.
// this struct holds all necessary parameters to generate a test filesystem
// with a specific structure and characteristics
type Layout struct {
    // rootdir specifies the base directory where the test filesystem will be created
    // this directory will contain all generated subdirectories and files
    RootDir string
    
    // numDirs specifies the total number of directories to create in the test filesystem
    // these directories will be created in a nested structure based on a square root calculation
    NumDirs int
    
    // numFiles specifies the total number of files to create and distribute
    // across the directory structure. files will be distributed evenly among
    // the leaf directories
    NumFiles int
    
    // fileSize specifies how large each generated file should be in bytes
    // all files will be created with this exact size and filled with random data
    FileSize int64
}

// NewLayout creates a new Layout with the provided configuration. The returned Layout
// can be used to generate a test filesystem with the specified characteristics.
func NewLayout(rootDir string, numDirs, numFiles int, fileSize int64) *Layout {
    return &Layout{
        RootDir:  rootDir,
        NumDirs:  numDirs,
        NumFiles: numFiles,
        FileSize: fileSize,
    }
}

// Create generates the complete test filesystem layout according to the
// configuration specified in the Layout struct. It creates a nested directory
// structure and distributes files across the directories.
func (l *Layout) Create() error {
    // calculate the number of directories per level using square root
    // this creates a balanced tree structure that's not too wide or deep
    dirsPerLevel := int(math.Ceil(math.Sqrt(float64(l.NumDirs))))

    // calculate the number of levels needed based on dirs per level
    // this uses log base dirsPerLevel of total dirs to determine depth
    numLevels := int(math.Ceil(math.Log(float64(l.NumDirs)) / math.Log(float64(dirsPerLevel))))

    // create the root directory with standard permissions
    // this will be the base of our test filesystem
    err := os.MkdirAll(l.RootDir, 0755)
    if err != nil {
        return fmt.Errorf("failed to create root directory: %w", err)
    }

    // create the nested directory structure
    // this returns a slice of all created directory paths for file distribution
    dirs, err := l.createDirectories(dirsPerLevel, numLevels)
    if err != nil {
        return fmt.Errorf("failed to create directory structure: %w", err)
    }

    // distribute files across the created directories
    // this ensures even distribution of files across all directories
    err = l.distributeFiles(dirs)
    if err != nil {
        return fmt.Errorf("failed to distribute files: %w", err)
    }

    return nil
}

// createDirectories generates the nested directory structure for the test filesystem.
// it uses a breadth-first approach to create a balanced directory tree, returning
// a slice of all created directory paths.
func (l *Layout) createDirectories(dirsPerLevel, numLevels int) ([]string, error) {
    // slice to store paths of all created directories
    // this will be used later for file distribution
    var dirs []string

    // queue for breadth-first directory creation
    // initialize with root directory as the starting point
    queue := []string{l.RootDir}
    currentLevel := 0
    dirsCreated := 0

    // create directories breadth-first until we reach target number
    // this ensures we create a balanced directory structure
    for len(queue) > 0 && dirsCreated < l.NumDirs {
        // get the next directory to process from the queue
        current := queue[0]
        queue = queue[1:]

        // only create subdirectories if we're not at a leaf level
        // this prevents creating deeper directories than necessary
        if currentLevel < numLevels-1 {
            // create subdirectories for current directory
            // limit by either dirsPerLevel or remaining dirs needed
            for i := 0; i < dirsPerLevel && dirsCreated < l.NumDirs; i++ {
                // generate a deterministic directory name that includes level and index
                // this makes the structure predictable and easy to navigate
                dirName := fmt.Sprintf("dir_l%d_%d", currentLevel+1, i)
                dirPath := filepath.Join(current, dirName)

                // create the directory with standard permissions
                err := os.Mkdir(dirPath, 0755)
                if err != nil {
                    return nil, fmt.Errorf("failed to create directory %s: %w", dirPath, err)
                }

                // track the new directory and add it to the processing queue
                dirs = append(dirs, dirPath)
                queue = append(queue, dirPath)
                dirsCreated++
            }
        }
    }

    return dirs, nil
}

// distributeFiles creates and distributes files across the provided directories
// files are evenly distributed using a deterministic pattern, with each file
// being filled with random data of the specified size
func (l *Layout) distributeFiles(dirs []string) error {
    // verify we have directories available for file distribution
    // this prevents attempting to create files with nowhere to put them
    if len(dirs) == 0 {
        return fmt.Errorf("no directories available for file distribution")
    }

    // create the specified number of files
    // distribute them evenly across available directories
    for i := 0; i < l.NumFiles; i++ {
        // determine target directory using modulo operation
        // this ensures even distribution across all directories
        targetDir := dirs[i%len(dirs)]

        // generate a deterministic file name that includes its index
        // this makes files easy to track and reference
        fileName := fmt.Sprintf("file_%d", i)
        filePath := filepath.Join(targetDir, fileName)

        // create the file using the LayoutTestFile function
        // this creates a file of the specified size filled with random data
        // pass false for reinitialize to reuse existing files
        err := LayoutTestFile(filePath, int(l.FileSize), false)
        if err != nil {
            return fmt.Errorf("failed to create file %s: %w", filePath, err)
        }
    }

    return nil
}

// LayoutTestFile creates a file of specified size filled with random data
// if reinitialize is true, recreate the file even if it exists
func LayoutTestFile(file string, size int, reinitialize bool) error {
    // check if file already exists with correct size
    if !reinitialize && CheckExistingFile(file, size) {
        // file exists and is valid, no need to recreate
        return nil
    }
    
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

// CheckExistingFile verifies if a file exists with the correct size and permissions
// returns true if file exists with correct size and is writable, false otherwise
func CheckExistingFile(file string, size int) bool {
    // get file information
    fileInfo, err := os.Stat(file)
    
    // if there's an error (including file not existing), return false
    if err != nil {
        return false
    }
    
    // check if the size matches what we expect
    if fileInfo.Size() != int64(size) {
        return false
    }
    
    // attempt to open the file for writing to verify permissions
    f, err := os.OpenFile(file, os.O_WRONLY, 0)
    
    // if we can't open the file for writing, return false
    if err != nil {
        return false
    }
    
    // close the file handle
    f.Close()
    
    // all checks passed, file exists and is writable
    return true
}

