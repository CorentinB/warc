package warc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
)

// Mode defines the way Reader will generate Records.
type Mode int

func (m Mode) String() string {
	switch m {
	case SequentialMode:
		return "SequentialMode"
	case AsynchronousMode:
		return "AsynchronousMode"
	}
	return ""
}

const (
	// SequentialMode means Records have to be consumed one by one and a call to
	// ReadRecord() invalidates the previous record. The benefit is that
	// Records have almost no overhead since they wrap around
	// the underlying Reader.
	SequentialMode Mode = iota
	// AsynchronousMode means calls to ReadRecord don't effect previously
	// returned Records. This mode copies the Record's content into
	// separate memory, thus bears memory overhead.
	AsynchronousMode
	// DefaultMode defines the reading mode used in NewReader().
	DefaultMode = AsynchronousMode
)

// Reader reads WARC records from WARC files.
type Reader struct {
	// Unexported fields.
	mode        Mode
	compression CompressionType
	source      io.ReadCloser
	reader      *bufio.Reader
	record      *Record
	buffer      []byte
}

// NewReaderMode is like NewReader, but specifies the mode instead of
// assuming DefaultMode.
func NewReaderMode(reader io.Reader, mode Mode) (*Reader, error) {
	compr, source, err := decompress(reader)
	if err != nil {
		return nil, err
	}
	return &Reader{
		mode:        mode,
		compression: compr,
		source:      source,
		reader:      bufio.NewReader(source),
		buffer:      make([]byte, 4096),
	}, nil
}

// Close closes the reader.
func (r *Reader) Close() {
	if r.source != nil {
		r.source.Close()
		r.source = nil
		r.reader = nil
		r.record = nil
	}
}

// readLine reads the next line in the opened WARC file.
func (r *Reader) readLine() (string, error) {
	data, isPrefix, err := r.reader.ReadLine()
	if err != nil {
		return "", err
	}

	// Line was too long for the buffer.
	// TODO: rather return an error in this case? This function
	// is only used on header fields and they shouldn't exceed the buffer size
	// or should they?
	if isPrefix {
		buffer := new(bytes.Buffer)
		buffer.Write(data)
		for isPrefix {
			data, isPrefix, err = r.reader.ReadLine()
			if err != nil {
				return "", err
			}
			buffer.Write(data)
		}
		return buffer.String(), nil
	}
	return string(data), nil
}

// ReadRecord reads the next record from the opened WARC file.
func (r *Reader) ReadRecord() (*Record, error) {
	// Go to the position of the next record in the file.
	r.seekRecord()

	// Skip the record version line.
	if _, err := r.readLine(); err != nil {
		return nil, err
	}

	// Parse the record header.
	header := NewHeader()
	for {
		line, err := r.readLine()
		if err != nil {
			return nil, err
		}
		if line == "" {
			break
		}
		if key, value := splitKeyValue(line); key != "" {
			header.Set(key, value)
		}
	}

	// Determine the content length and then retrieve the record content.
	length, err := strconv.Atoi(header["content-length"])
	if err != nil {
		return nil, fmt.Errorf("failed to parse field Content-Length: %v", err)
	}
	content, err := sliceReader(r.reader, length, r.mode == AsynchronousMode)
	if err != nil {
		return nil, err
	}
	r.record = &Record{
		Header:  header,
		Content: content,
	}
	return r.record, nil
}

// seekRecord moves the Reader to the position of the next WARC record
// in the opened WARC file.
func (r *Reader) seekRecord() error {
	// No record was read yet? The r.reader must be at a start of the file and
	// thus the start of a record.
	if r.record == nil {
		return nil
	}

	// If the mode is set to SequentialMode, the underlying r.reader might be
	// anywhere inside the active record's block - depending on how much the
	// user actually consumed. So we have to make sure all content gets skipped
	// here.
	if r.mode == SequentialMode {
		for {
			n, err := r.record.Content.Read(r.buffer)
			if n == 0 || err != nil {
				break
			}
		}
	}

	// Set to nil so it's safe to call this function several times without
	// destroying stuff.
	r.record = nil
	for i := 0; i < 2; i++ {
		line, err := r.readLine()
		if err != nil {
			return err
		}
		if line != "" {
			return fmt.Errorf("expected empty line, got %q", line)
		}
	}
	return nil
}

// Mode returns the reader mode.
func (r *Reader) Mode() Mode {
	return r.mode
}

// sliceReader returns a new io.Reader for the next n bytes in source.
// If clone is true, the n bytes will be fully read from source and the
// resulting io.Reader will have its own copy of the data. Calls to the
// result's Read() function won't change the state of source.
// If clone is false, no bytes will be consumed from source and the resulting
// io.Reader will wrap itself around source. Each call to the result's Read()
// function will change the state of source.
func sliceReader(source io.Reader, size int, clone bool) (io.Reader, error) {
	reader := io.LimitReader(source, int64(size))
	if !clone {
		return reader, nil
	}
	content, err := ioutil.ReadAll(reader)
	if err != nil {
		return nil, err
	}
	return bytes.NewReader(content), nil
}

// splitKeyValue parses WARC record header fields.
func splitKeyValue(line string) (string, string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}
