package warc

// The following code is heavily inspired by: https://github.com/tgulacsi/go/blob/master/temp/memfile.go

import (
	"bytes"
	"errors"
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
	reading         bool // transitions at most once from false -> true
	closed          bool
}

// ReadWriteSeekCloser is an io.Writer + io.Reader + io.Seeker + io.Closer.
type ReadWriteSeekCloser interface {
	ReadSeekCloser
	io.Writer
}

// NewSpooledTempFile returns an ReadWriteSeekCloser,
// with some important constraints:
// you can Write into it, but whenever you call Read or Seek on it,
// Write is forbidden, will return an error.
// If threshold is -1, then the default MaxInMemorySize is used.
func NewSpooledTempFile(filePrefix string, tempDir string, threshold int, fullOnDisk bool) ReadWriteSeekCloser {
	if threshold < 0 {
		threshold = MaxInMemorySize
	}

	return &spooledTempFile{
		filePrefix:      filePrefix,
		tempDir:         tempDir,
		buf:             spooledPool.Get().(*bytes.Buffer),
		maxInMemorySize: threshold,
		fullOnDisk:      fullOnDisk,
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

	if s.file != nil {
		n, err = s.file.Write(p)
		return
	}

	if (s.buf.Len()+len(p) > s.maxInMemorySize) || s.fullOnDisk {
		s.file, err = os.CreateTemp(s.tempDir, s.filePrefix+"-")
		if err != nil {
			return
		}

		_, err = io.Copy(s.file, s.buf)
		if err != nil {
			s.file.Close()
			s.file = nil
			return
		}

		s.buf.Reset()
		spooledPool.Put(s.buf)
		s.buf = nil

		if n, err = s.file.Write(p); err != nil {
			s.file.Close()
			s.file = nil
		}

		return
	}

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
	} else {
		return ""
	}
}
