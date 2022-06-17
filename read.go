package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
)

// Reader store the bufio.Reader and gzip.Reader for a WARC file
type Reader struct {
	reader     *bufio.Reader
	gzipReader *gzip.Reader
	record     *Record
}

// NewReader returns a new WARC reader
func NewReader(reader io.Reader) (*Reader, error) {
	bufioReader := bufio.NewReader(reader)

	// Wrap the reader into a bufio.Reader to add the ByteReader method
	zr, err := gzip.NewReader(bufioReader)
	if err != nil {
		return nil, err
	}

	return &Reader{
		reader:     bufioReader,
		gzipReader: zr,
	}, nil
}

// Close closes the reader.
func (r *Reader) Close() {
	r.gzipReader.Close()
}

type reader interface {
	ReadString(delim byte) (line string, err error)
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

// ReadRecord reads the next record from the opened WARC file.
// If onDisk is set to true, then the record's payload will be
// written to a temp file on disk, and specified in the *Record.PayloadPath,
// else, everything happen in memory.
func (r *Reader) ReadRecord() (*Record, error) {
	var err error
	var tempReader *bufio.Reader

	r.gzipReader.Multistream(false)
	tempReader = bufio.NewReader(r.gzipReader)

	// Skip first line (WARC version)
	// TODO: add check for WARC version
	_, err = readUntilDelim(tempReader, []byte("\r\n"))
	if err != nil {
		if err == io.EOF {
			return &Record{Header: nil, Content: nil}, err
		}
		return nil, err
	}

	// Parse the record header
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

	var buf bytes.Buffer
	if _, err := buf.ReadFrom(tempReader); err != nil {
		return nil, err
	}

	buf.Truncate(bytes.Index(buf.Bytes(), []byte("\r\n\r\n")))

	r.record = &Record{
		Header:  header,
		Content: &buf,
	}

	// Reset the reader for the next block
	err = r.gzipReader.Reset(r.reader)
	if err == io.EOF {
		return r.record, nil
	}
	if err != nil {
		return r.record, err
	}

	return r.record, nil
}
