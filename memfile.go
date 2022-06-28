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
	"time"
)

// MaxInMemorySize is the max number of bytes
// (currently 2MB) to hold in memory before starting
// to write to disk
var MaxInMemorySize = 2097152

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
	Stat() (os.FileInfo, error)
	FileName() string
}

// spooledTempFile writes to memory (or to disk if
// over MaxInMemorySize) and deletes the file on Close
type spooledTempFile struct {
	stat            os.FileInfo
	buf             *bytes.Buffer
	mem             *bytes.Reader
	file            *os.File
	filePrefix      string
	maxInMemorySize int
	reading         bool // transitions at most once from false -> true
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
func NewSpooledTempFile(filePrefix string) ReadWriteSeekCloser {
	return &spooledTempFile{
		filePrefix: filepath.Base(filePrefix),
		buf:        new(bytes.Buffer),
	}
}

func (ms *spooledTempFile) prepareRead() error {
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
	ms.buf = nil

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

func (ms *spooledTempFile) ReadFrom(r io.Reader) (n int64, err error) {
	if ms.reading {
		panic("write after read")
	}

	if ms.maxInMemorySize <= 0 {
		ms.maxInMemorySize = MaxInMemorySize
	}

	var size int64
	if fh, ok := r.(*os.File); ok {
		if ms.stat, err = fh.Stat(); err == nil {
			size = ms.stat.Size()
		}
	}

	if ms.file == nil && size > 0 && size < int64(ms.maxInMemorySize) {
		return io.Copy(ms.buf, r)
	}

	if ms.file == nil {
		ms.file, err = ioutil.TempFile("", "-"+ms.filePrefix)
		if err != nil {
			return 0, err
		}

		os.Remove(ms.file.Name())
	}

	if n, err = io.Copy(ms.file, r); err != nil {
		ms.file.Close()
		ms.file = nil
	}

	return n, err
}

func (ms *spooledTempFile) Write(p []byte) (n int, err error) {
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
		ms.file, err = ioutil.TempFile("", "-"+ms.filePrefix)
		if err != nil {
			return
		}

		os.Remove(ms.file.Name())
		_, err = io.Copy(ms.file, ms.buf)
		if err != nil {
			ms.file.Close()
			ms.file = nil
			return
		}

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
	f := ms.file

	ms.file, ms.mem, ms.buf = nil, nil, nil
	if f == nil {
		return nil
	}

	f.Close()

	if err := os.Remove(f.Name()); err != nil && !strings.Contains(err.Error(), "exist") {
		return err
	}

	return nil
}

func (ms *spooledTempFile) FileName() string {
	if ms.file != nil {
		return ms.file.Name()
	} else {
		return ""
	}
}

func (ms *spooledTempFile) Stat() (os.FileInfo, error) {
	return ms.stat, nil
}

type dummyFileInfo struct {
	mtime time.Time
	name  string
	size  int64
	mode  os.FileMode
	isDir bool
}

func (dfi dummyFileInfo) Name() string {
	return dfi.name
}
func (dfi dummyFileInfo) Size() int64 {
	return dfi.size
}
func (dfi dummyFileInfo) Mode() os.FileMode {
	return dfi.mode
}
func (dfi dummyFileInfo) ModTime() time.Time {
	return dfi.mtime
}
func (dfi dummyFileInfo) IsDir() bool {
	return dfi.isDir
}
func (dfi dummyFileInfo) Sys() interface{} {
	return nil
}
