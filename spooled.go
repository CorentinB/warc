package warc

// The following code is heavily inspired by: https://github.com/tgulacsi/go/blob/master/temp/memfile.go

import (
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"path/filepath"
	"strings"
	"sync"
)

// MaxInMemorySize is the max number of bytes
// (currently 500KB) to hold in memory before starting
// to write to disk
const MaxInMemorySize = 512000

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
func NewSpooledTempFile(filePrefix string, tempDir string) ReadWriteSeekCloser {
	return &spooledTempFile{
		filePrefix: filepath.Base(filePrefix),
		tempDir:    tempDir,
		buf:        spooledPool.Get().(*bytes.Buffer),
	}
}

func (ms *spooledTempFile) prepareRead() error {
	if ms.closed {
		return io.EOF
	}

	if ms.reading && (ms.file != nil || ms.buf == nil || ms.mem != nil) {
		return nil
	}

	ms.reading = true
	if ms.file != nil {
		if _, err := ms.file.Seek(0, 0); err != nil {
			return fmt.Errorf("file=%v: %w", ms.file, err)
		}
		return nil
	}

	ms.mem = bytes.NewReader(ms.buf.Bytes())

	return nil
}

func (ms *spooledTempFile) Read(p []byte) (n int, err error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}

	if ms.file != nil {
		return ms.file.Read(p)
	}

	return ms.mem.Read(p)
}

func (ms *spooledTempFile) ReadAt(p []byte, off int64) (n int, err error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}

	if ms.file != nil {
		return ms.file.ReadAt(p, off)
	}

	return ms.mem.ReadAt(p, off)
}

func (ms *spooledTempFile) Seek(offset int64, whence int) (int64, error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}

	if ms.file != nil {
		return ms.file.Seek(offset, whence)
	}

	return ms.mem.Seek(offset, whence)
}

func (ms *spooledTempFile) Write(p []byte) (n int, err error) {
	if ms.closed {
		return 0, io.EOF
	}

	if ms.reading {
		panic("write after read")
	}

	if ms.file != nil {
		n, err = ms.file.Write(p)
		return
	}

	if ms.maxInMemorySize <= 0 {
		ms.maxInMemorySize = MaxInMemorySize
	}

	if ms.buf.Len()+len(p) > ms.maxInMemorySize {
		ms.file, err = ioutil.TempFile(ms.tempDir, ms.filePrefix+"-")
		if err != nil {
			return
		}

		_, err = io.Copy(ms.file, ms.buf)
		if err != nil {
			ms.file.Close()
			ms.file = nil
			return
		}

		ms.buf.Reset()
		spooledPool.Put(ms.buf)
		ms.buf = nil

		if n, err = ms.file.Write(p); err != nil {
			ms.file.Close()
			ms.file = nil
		}

		return
	}

	return ms.buf.Write(p)
}

func (ms *spooledTempFile) Close() error {
	ms.closed = true
	ms.mem = nil

	if ms.buf != nil {
		ms.buf.Reset()
		spooledPool.Put(ms.buf)
		ms.buf = nil
	}

	if ms.file == nil {
		return nil
	}

	ms.file.Close()

	if err := os.Remove(ms.file.Name()); err != nil && !strings.Contains(err.Error(), "exist") {
		return err
	}

	ms.file = nil

	return nil
}

func (ms *spooledTempFile) FileName() string {
	if ms.file != nil {
		return ms.file.Name()
	} else {
		return ""
	}
}
