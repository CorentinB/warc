package warc

import (
	"bufio"
	"bytes"
	"fmt"
	"io"
	"strconv"
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
func (r *Reader) ReadRecord() (*Record, error) {
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
			return &Record{Header: nil, Content: nil}, err
		}
		return nil, err
	}

	// Parse the record headers
	header := NewHeader()
	for {
		line, err := readUntilDelim(tempReader, []byte("\r\n"))
		if err != nil {
			return nil, err
		}
		if len(line) == 0 {
			break
		}
		if key, value := splitKeyValue(string(line)); key != "" {
			header.Set(key, value)
		}
	}

	// Get the content length
	length, err := strconv.Atoi(header.Get("Content-Length"))
	if err != nil {
		return nil, fmt.Errorf("invalid content length: %w", err)
	}

	// Use a buffer to read the content directly
	content := make([]byte, length)
	_, err = io.ReadFull(tempReader, content)
	if err != nil {
		return nil, err
	}

	buf := NewSpooledTempFile("warc", "", false)
	_, err = buf.Write(content)
	if err != nil {
		return nil, err
	}

	r.record = &Record{
		Header:  header,
		Content: buf,
		Version: string(warcVer),
	}

	// Skip two empty lines
	for i := 0; i < 2; i++ {
		boundary, _, err := r.bufReader.ReadLine()
		if (err != nil) && (err != io.EOF) {
			return nil, fmt.Errorf("read record boundary: %w", err)
		}

		if len(boundary) != 0 {
			return nil, fmt.Errorf("non-empty record boundary [boundary: %s]", boundary)
		}
	}

	return r.record, nil
}
