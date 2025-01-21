package spooledtempfile

import (
	"bufio"
	"bytes"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"
)

// MaxInMemorySize is the max number of bytes (currently 1MB)
// to hold in memory before starting to write to disk
const MaxInMemorySize = 1024 * 1024

// DefaultMaxRAMUsageFraction is the default fraction of system RAM above which
// we'll force spooling to disk. For example, 0.5 = 50%.
const DefaultMaxRAMUsageFraction = 0.50

// Constant defining how often we check system memory usage.
const memoryCheckInterval = 500 * time.Millisecond

// globalMemoryCache is a struct representing global cache of memory usage data.
type globalMemoryCache struct {
	sync.Mutex
	lastChecked  time.Time
	lastFraction float64
}

// memoryUsageCache is an atomic pointer to memoryUsageData.
var memoryUsageCache *globalMemoryCache

var spooledPool = sync.Pool{
	New: func() interface{} {
		return bytes.NewBuffer(nil)
	},
}

// ReaderAt is the interface for ReadAt - read at position, without moving pointer.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

// ReadSeekCloser is an io.Reader + ReaderAt + io.Seeker + io.Closer + Stat
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	ReaderAt
	io.Closer
	FileName() string
	Len() int
}

// spooledTempFile writes to memory (or to disk if
// over MaxInMemorySize) and deletes the file on Close
type spooledTempFile struct {
	buf                 *bytes.Buffer
	mem                 *bytes.Reader
	file                *os.File
	filePrefix          string
	tempDir             string
	maxInMemorySize     int
	fullOnDisk          bool
	reading             bool // transitions at most once from false -> true
	closed              bool
	maxRAMUsageFraction float64 // fraction above which we skip in-memory buffering
}

// ReadWriteSeekCloser is an io.Writer + io.Reader + io.Seeker + io.Closer.
type ReadWriteSeekCloser interface {
	ReadSeekCloser
	io.Writer
}

// NewSpooledTempFile returns an ReadWriteSeekCloser,
// with some important constraints:
//   - You can Write into it, but whenever you call Read or Seek on it,
//     subsequent Write calls will panic.
//   - If threshold is -1, then the default MaxInMemorySize is used.
//   - If maxRAMUsageFraction <= 0, we default to DefaultMaxRAMUsageFraction. E.g. 0.5 = 50%.
//
// If the system memory usage is above maxRAMUsageFraction, we skip writing
// to memory and spool directly on disk to avoid OOM scenarios in high concurrency.
//
// From our tests, we've seen that a bytes.Buffer minimum allocated size is 64 bytes, any threshold below that will cause the first write to be spooled on disk.
func NewSpooledTempFile(
	filePrefix string,
	tempDir string,
	threshold int,
	fullOnDisk bool,
	maxRAMUsageFraction float64,
) ReadWriteSeekCloser {
	if threshold < 0 {
		threshold = MaxInMemorySize
	}
	if maxRAMUsageFraction <= 0 {
		maxRAMUsageFraction = DefaultMaxRAMUsageFraction
	}

	return &spooledTempFile{
		filePrefix:          filePrefix,
		tempDir:             tempDir,
		buf:                 spooledPool.Get().(*bytes.Buffer),
		maxInMemorySize:     threshold,
		fullOnDisk:          fullOnDisk,
		maxRAMUsageFraction: maxRAMUsageFraction,
	}
}

func (s *spooledTempFile) prepareRead() error {
	if s.closed {
		return io.EOF
	}

	if s.reading && (s.file != nil || s.buf == nil || s.mem != nil) {
		return nil
	}

	s.reading = true
	if s.file != nil {
		if _, err := s.file.Seek(0, 0); err != nil {
			return fmt.Errorf("file=%v: %w", s.file, err)
		}
		return nil
	}

	s.mem = bytes.NewReader(s.buf.Bytes())
	return nil
}

func (s *spooledTempFile) Len() int {
	if s.file != nil {
		fi, err := s.file.Stat()
		if err != nil {
			return -1
		}
		return int(fi.Size())
	}
	return s.buf.Len()
}

func (s *spooledTempFile) Read(p []byte) (n int, err error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}

	if s.file != nil {
		return s.file.Read(p)
	}
	return s.mem.Read(p)
}

func (s *spooledTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}

	if s.file != nil {
		return s.file.ReadAt(p, off)
	}
	return s.mem.ReadAt(p, off)
}

func (s *spooledTempFile) Seek(offset int64, whence int) (int64, error) {
	if err := s.prepareRead(); err != nil {
		return 0, err
	}

	if s.file != nil {
		return s.file.Seek(offset, whence)
	}
	return s.mem.Seek(offset, whence)
}

func (s *spooledTempFile) Write(p []byte) (n int, err error) {
	if s.closed {
		return 0, io.EOF
	}
	if s.reading {
		panic("write after read")
	}

	// If we already have a file open, we always write to disk.
	if s.file != nil {
		return s.file.Write(p)
	}

	// Otherwise, check if system memory usage is above threshold
	// or if we've exceeded our own in-memory limit, or if user forced on-disk.
	aboveRAMThreshold := s.isSystemMemoryUsageHigh()
	if aboveRAMThreshold || s.fullOnDisk || (s.buf.Len()+len(p) > s.maxInMemorySize) || (s.buf.Cap() > s.maxInMemorySize) {
		// Switch to file if we haven't already
		s.file, err = os.CreateTemp(s.tempDir, s.filePrefix+"-")
		if err != nil {
			return 0, err
		}

		// Copy what we already had in the buffer
		_, err = io.Copy(s.file, s.buf)
		if err != nil {
			s.file.Close()
			s.file = nil
			return 0, err
		}

		// If we're above the RAM threshold, we don't want to keep the buffer around.
		if s.buf.Cap() > s.maxInMemorySize {
			s.buf = nil
		} else {
			// Release the buffer
			s.buf.Reset()
			spooledPool.Put(s.buf)
			s.buf = nil
		}

		// Write incoming bytes directly to file
		n, err = s.file.Write(p)
		if err != nil {
			s.file.Close()
			s.file = nil
			return n, err
		}
		return n, nil
	}

	// Otherwise, stay in memory.
	return s.buf.Write(p)
}

func (s *spooledTempFile) Close() error {
	s.closed = true
	s.mem = nil

	// If we're above the RAM threshold, we don't want to keep the buffer around.
	if s.buf != nil && s.buf.Cap() > s.maxInMemorySize {
		s.buf = nil
	} else if s.buf != nil {
		// Release the buffer
		s.buf.Reset()
		spooledPool.Put(s.buf)
		s.buf = nil
	}

	if s.file == nil {
		return nil
	}

	s.file.Close()

	if err := os.Remove(s.file.Name()); err != nil && !errors.Is(err, os.ErrNotExist) {
		return err
	}
	s.file = nil
	return nil
}

func (s *spooledTempFile) FileName() string {
	if s.file != nil {
		return s.file.Name()
	}
	return ""
}

// isSystemMemoryUsageHigh returns true if current memory usage
// exceeds s.maxRAMUsageFraction of total system memory.
// This implementation is Linux-specific via /proc/meminfo.
func (s *spooledTempFile) isSystemMemoryUsageHigh() bool {
	usedFraction, err := getCachedMemoryUsage()
	if err != nil {
		// If we fail to get memory usage info, we conservatively return false,
		// or you may choose to return true to avoid in-memory usage.
		return false
	}
	return usedFraction >= s.maxRAMUsageFraction
}

func getCachedMemoryUsage() (float64, error) {
	if memoryUsageCache == nil {
		memoryUsageCache = &globalMemoryCache{}
	}

	memoryUsageCache.Lock()
	defer memoryUsageCache.Unlock()

	// 1) If it's still fresh, just return the cached value.
	if time.Since(memoryUsageCache.lastChecked) < memoryCheckInterval {
		return memoryUsageCache.lastFraction, nil
	}

	// 2) Otherwise, do a fresh read (expensive).
	fraction, err := getSystemMemoryUsedFraction()
	if err != nil {
		return 0, err
	}

	// 3) Update the cache
	memoryUsageCache.lastChecked = time.Now()
	memoryUsageCache.lastFraction = fraction

	return fraction, nil
}

// getSystemMemoryUsedFraction parses /proc/meminfo on Linux to figure out
// how much memory is used vs total. Returns fraction = used / total
// This is a Linux-specific implementation.
// This function is defined as a variable so it can be overridden in tests.
// Now includes lock-free CAS caching to avoid hammering /proc/meminfo on every call.
var getSystemMemoryUsedFraction = func() (float64, error) {
	// We're the winners and need to refresh the data.
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		// If we cannot open /proc/meminfo, return an error
		// or fallback if you prefer
		return 0, fmt.Errorf("failed to open /proc/meminfo: %v", err)
	}
	defer f.Close()

	var memTotal, memAvailable, memFree, buffers, cached uint64
	scanner := bufio.NewScanner(f)
	for scanner.Scan() {
		line := scanner.Text()
		fields := strings.Fields(line)
		if len(fields) < 2 {
			continue
		}
		key := strings.TrimRight(fields[0], ":")
		value, _ := strconv.ParseUint(fields[1], 10, 64)
		// value is typically in kB
		switch key {
		case "MemTotal":
			memTotal = value
		case "MemAvailable":
			memAvailable = value
		case "MemFree":
			memFree = value
		case "Buffers":
			buffers = value
		case "Cached":
			cached = value
		}
	}
	if err := scanner.Err(); err != nil {
		return 0, fmt.Errorf("scanner error reading /proc/meminfo: %v", err)
	}

	if memTotal == 0 {
		return 0, fmt.Errorf("could not find MemTotal in /proc/meminfo")
	}

	var used uint64
	if memAvailable > 0 {
		// Linux 3.14+ has MemAvailable for better measure
		used = memTotal - memAvailable
	} else {
		// Approximate available as free + buffers + cached
		approxAvailable := memFree + buffers + cached
		used = memTotal - approxAvailable
	}

	fraction := float64(used) / float64(memTotal)

	return fraction, nil
}
