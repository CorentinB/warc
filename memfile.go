/*
Copyright 2013, 2020 the Camlistore authors.

Licensed under the Apache License, Version 2.0 (the "License");
you may not use this file except in compliance with the License.
You may obtain a copy of the License at

     http://www.apache.org/licenses/LICENSE-2.0

Unless required by applicable law or agreed to in writing, software
distributed under the License is distributed on an "AS IS" BASIS,
WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
See the License for the specific language governing permissions and
limitations under the License.
*/

//https://github.com/tgulacsi/go/blob/master/temp/memfile.go

package warc

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

// MaxInMemorySlurp is the threshold for in-memory or on-disk storage
// for slurped data.
var MaxInMemorySlurp = 4 << 20 // 4MB.  *shrug*.

// ReaderAt is the interface for ReadAt - read at position, without moving pointer.
type ReaderAt interface {
	ReadAt(p []byte, off int64) (n int, err error)
}

// Stater is the interface for os.Stat.
type Stater interface {
	Stat() (os.FileInfo, error)
}

// ReadSeekCloser is an io.Reader + ReaderAt + io.Seeker + io.Closer + Stater
type ReadSeekCloser interface {
	io.Reader
	io.Seeker
	ReaderAt
	io.Closer
	Stat() (os.FileInfo, error)
}

// MakeReadSeekCloser makes an io.ReadSeeker + io.Closer by reading the whole reader
// If the given Reader is a Closer, too, than that Close will be called
func MakeReadSeekCloser(blobRef string, r io.Reader) (ReadSeekCloser, error) {
	if rsc, ok := r.(ReadSeekCloser); ok {
		return rsc, nil
	}

	ms := NewMemorySlurper(blobRef).(*memorySlurper)
	n, err := io.Copy(ms, r)
	if err != nil {
		return nil, fmt.Errorf("copy from %v to %v: %w", r, ms, err)
	}

	if ms.stat == nil {
		if ms.file == nil {
			ms.stat = dummyFileInfo{name: "memory", size: n, mtime: time.Now()}
		} else {
			ms.stat, err = ms.file.Stat()
		}
	}
	return ms, err
}

// NewReadSeeker is a convenience function of MakeReadSeekCloser.
func NewReadSeeker(r io.Reader) (ReadSeekCloser, error) {
	return MakeReadSeekCloser("", r)
}

// memorySlurper slurps up a blob to memory (or spilling to disk if
// over MaxInMemorySlurp) and deletes the file on Close
type memorySlurper struct {
	stat             os.FileInfo
	buf              *bytes.Buffer
	mem              *bytes.Reader
	file             *os.File // nil until allocated
	blobRef          string   // only used for tempfile's prefix
	maxInMemorySlurp int
	reading          bool // transitions at most once from false -> true
}

// ReadWriteSeekCloser is an io.Writer + io.Reader + io.Seeker + io.Closer.
type ReadWriteSeekCloser interface {
	ReadSeekCloser
	io.Writer
}

// NewMemorySlurper returns an ReadWriteSeekCloser,
// with some important constraints:
// you can Write into it, but whenever you call Read or Seek on it,
// Write is forbidden, will return an error.
func NewMemorySlurper(blobRef string) ReadWriteSeekCloser {
	return &memorySlurper{
		blobRef: filepath.Base(blobRef),
		buf:     new(bytes.Buffer),
	}
}

func (ms *memorySlurper) prepareRead() error {
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

func (ms *memorySlurper) Read(p []byte) (n int, err error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}
	if ms.file != nil {
		return ms.file.Read(p)
	}
	return ms.mem.Read(p)
}

func (ms *memorySlurper) ReadAt(p []byte, off int64) (n int, err error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}
	if ms.file != nil {
		return ms.file.ReadAt(p, off)
	}
	return ms.mem.ReadAt(p, off)
}

func (ms *memorySlurper) Seek(offset int64, whence int) (int64, error) {
	if err := ms.prepareRead(); err != nil {
		return 0, err
	}
	if ms.file != nil {
		return ms.file.Seek(offset, whence)
	}
	return ms.mem.Seek(offset, whence)
}

func (ms *memorySlurper) ReadFrom(r io.Reader) (n int64, err error) {
	if ms.reading {
		panic("write after read")
	}
	if ms.maxInMemorySlurp <= 0 {
		ms.maxInMemorySlurp = MaxInMemorySlurp
	}
	var size int64
	if fh, ok := r.(*os.File); ok {
		if ms.stat, err = fh.Stat(); err == nil {
			size = ms.stat.Size()
		}
	}
	if ms.file == nil && size > 0 && size < int64(ms.maxInMemorySlurp) {
		return io.Copy(ms.buf, r)
	}
	if ms.file == nil {
		ms.file, err = ioutil.TempFile("", "memorySlurper-"+ms.blobRef)
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

func (ms *memorySlurper) Write(p []byte) (n int, err error) {
	if ms.reading {
		panic("write after read")
	}
	if ms.file != nil {
		n, err = ms.file.Write(p)
		return
	}

	if ms.maxInMemorySlurp <= 0 {
		ms.maxInMemorySlurp = MaxInMemorySlurp
	}
	if ms.buf.Len()+len(p) > ms.maxInMemorySlurp {
		ms.file, err = ioutil.TempFile("", "memorySlurper-"+ms.blobRef)
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

func (ms *memorySlurper) Cleanup() error {
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

func (ms *memorySlurper) Close() error {
	return ms.Cleanup()
}

func (ms *memorySlurper) Stat() (os.FileInfo, error) {
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
