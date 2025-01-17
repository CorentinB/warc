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
)

// MaxInMemorySize is the max number of bytes (currently 1MB)
// to hold in memory before starting to write to disk
const MaxInMemorySize = 1024 * 1024

// DefaultMaxRAMUsageFraction is the default fraction of system RAM above which
// we'll force spooling to disk. For example, 0.5 = 50%.
const DefaultMaxRAMUsageFraction = 0.50

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
	if aboveRAMThreshold || s.fullOnDisk || (s.buf.Len()+len(p) > s.maxInMemorySize) {
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

		// Release the buffer
		s.buf.Reset()
		spooledPool.Put(s.buf)
		s.buf = nil

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

	if s.buf != nil {
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
	usedFraction, err := getSystemMemoryUsedFraction()
	if err != nil {
		// If we fail to get memory usage info, we conservatively return false,
		// or you may choose to return true to avoid in-memory usage.
		return false
	}
	return usedFraction >= s.maxRAMUsageFraction
}

// getSystemMemoryUsedFraction parses /proc/meminfo on Linux to figure out
// how much memory is used vs total. Returns fraction = used / total
// This is a Linux-specific implementation.
// This function is defined as a variable so it can be overridden in tests.
var getSystemMemoryUsedFraction = func() (float64, error) {
	f, err := os.Open("/proc/meminfo")
	if err != nil {
		return 0, err
	}
	defer f.Close()

	// We look for MemTotal, MemAvailable (or MemFree if MemAvailable is missing)
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

	if memTotal == 0 {
		return 0, fmt.Errorf("could not find MemTotal in /proc/meminfo")
	}

	// If MemAvailable is present (Linux 3.14+), we can directly use it:
	if memAvailable > 0 {
		used := memTotal - memAvailable
		return float64(used) / float64(memTotal), nil
	}

	// Otherwise, approximate "available" as free+buffers+cached
	approxAvailable := memFree + buffers + cached
	used := memTotal - approxAvailable
	return float64(used) / float64(memTotal), nil
}
