package warc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"

	"github.com/CorentinB/warc/pkg/spooledtempfile"
)

// Reader store the bufio.Reader and gzip.Reader for a WARC file
type Reader struct {
	bufReader *bufio.Reader
	record    *Record
}

type reader interface {
	ReadString(delim byte) (line string, err error)
}

// NewReader returns a new WARC reader
func NewReader(reader io.ReadCloser) (*Reader, error) {
	decReader, err := NewDecompressionReader(reader)
	if err != nil {
		return nil, err
	}
	bufioReader := bufio.NewReader(decReader)

	return &Reader{
		bufReader: bufioReader,
	}, nil
}

func readUntilDelim(r reader, delim []byte) (line []byte, err error) {
	for {
		s := ""
		s, err = r.ReadString(delim[len(delim)-1])
		if err != nil {
			line = append(line, []byte(s)...)
			return line, err
		}

		line = append(line, []byte(s)...)
		if bytes.HasSuffix(line, delim) {
			return line[:len(line)-len(delim)], nil
		}
	}
}

// ReadRecord reads the next record from the opened WARC file
// returns:
//   - Record: if an error occurred, record **may be** nil. if eol is true, record **must be** nil.
//   - bool (eol): if true, we readed all records successfully.
//   - error: error
func (r *Reader) ReadRecord() (*Record, bool, error) {
	var (
		err        error
		tempReader *bufio.Reader
	)

	tempReader = bufio.NewReader(r.bufReader)

	// first line: WARC version
	var warcVer []byte
	warcVer, err = readUntilDelim(tempReader, []byte("\r\n"))
	if err != nil {
		if err == io.EOF {
			return nil, true, nil // EOF, no error
		}
		return nil, false, fmt.Errorf("reading WARC version: %w", err)
	}

	// Parse the record headers
	header := NewHeader()
	for {
		line, err := readUntilDelim(tempReader, []byte("\r\n"))
		if err != nil {
			return nil, false, fmt.Errorf("reading header: %w", err)
		}
		if len(line) == 0 {
			break
		}
		if key, value := splitKeyValue(string(line)); key != "" {
			header.Set(key, value)
		}
	}

	// Get the Content-Length
	length, err := strconv.ParseInt(header.Get("Content-Length"), 10, 64)
	if err != nil {
		return nil, false, fmt.Errorf("parsing Content-Length: %w", err)
	}

	// reading doesn't really need to be in TempDir, nor can we access it as it's on the client.
	buf := spooledtempfile.NewSpooledTempFile("warc", "", -1, false, -1)
	_, err = io.CopyN(buf, tempReader, length)
	if err != nil {
		return nil, false, fmt.Errorf("copying record content: %w", err)
	}

	r.record = &Record{
		Header:  header,
		Content: buf,
		Version: string(warcVer),
	}

	// Skip two empty lines
	for i := 0; i < 2; i++ {
		boundary, _, err := r.bufReader.ReadLine()
		if err != nil {
			if err == io.EOF {
				// record shall consist of a record header followed by a record content block and two newlines
				return r.record, false, fmt.Errorf("early EOF record boundary: %w", err)
			}
			return r.record, false, fmt.Errorf("reading record boundary: %w", err)
		}

		if len(boundary) != 0 {
			return r.record, false, fmt.Errorf("non-empty record boundary [boundary: %s]", boundary)
		}
	}

	return r.record, false, nil // ok
}
