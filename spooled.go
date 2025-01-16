package warc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"sync"
)

// MaxInMemorySize is the max number of bytes (currently 1MB)
// to hold in memory before starting to write to disk
const MaxInMemorySize = 1000000

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
	buf             *bytes.Buffer
	mem             *bytes.Reader
	file            *os.File
	filePrefix      string
	tempDir         string
	maxInMemorySize int
	fullOnDisk      bool
	reading         bool
	closed          bool
	manager         *SpoolManager
}

// ReadWriteSeekCloser is an io.Writer + io.Reader + io.Seeker + io.Closer.
type ReadWriteSeekCloser interface {
	ReadSeekCloser
	io.Writer
}

// NewSpooledTempFile returns an ReadWriteSeekCloser.
// If threshold is -1, then the default MaxInMemorySize is used.
func NewSpooledTempFile(filePrefix string, tempDir string, threshold int, fullOnDisk bool) ReadWriteSeekCloser {
	if threshold < 0 {
		threshold = MaxInMemorySize
	}

	s := &spooledTempFile{
		filePrefix:      filePrefix,
		tempDir:         tempDir,
		buf:             spooledPool.Get().(*bytes.Buffer),
		maxInMemorySize: threshold,
		fullOnDisk:      fullOnDisk,
		manager:         DefaultSpoolManager,
	}

	s.manager.RegisterSpool(s)

	return s
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
	if s.file != nil {
		return s.file.Write(p)
	}
	proposedSize := s.buf.Len() + len(p)
	if s.fullOnDisk ||
		proposedSize > s.maxInMemorySize {
		if err := s.switchToFile(); err != nil {
			return 0, err
		}
		return s.file.Write(p)
	}
	s.manager.AddBytes(len(p))
	return s.buf.Write(p)
}

func (s *spooledTempFile) switchToFile() error {
	f, err := os.CreateTemp(s.tempDir, s.filePrefix+"-")
	if err != nil {
		return err
	}
	if _, err = io.Copy(f, s.buf); err != nil {
		f.Close()
		return err
	}
	s.manager.SubBytes(s.buf.Len())
	s.buf.Reset()
	spooledPool.Put(s.buf)
	s.buf = nil
	s.file = f
	return nil
}

func (s *spooledTempFile) forceToDiskIfInMemory() {
	if s.file == nil && !s.closed {
		_ = s.switchToFile()
	}
}

func (s *spooledTempFile) Close() error {
	if s.closed {
		return nil
	}
	s.closed = true
	s.mem = nil
	if s.buf != nil {
		s.manager.SubBytes(s.buf.Len())
		s.buf.Reset()
		spooledPool.Put(s.buf)
		s.buf = nil
	}
	if s.file != nil {
		s.file.Close()
		os.Remove(s.file.Name())
		s.file = nil
	}
	s.manager.UnregisterSpool(s)
	return nil
}

func (s *spooledTempFile) FileName() string {
	if s.file != nil {
		return s.file.Name()
	}
	return ""
}
