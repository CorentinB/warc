package warc

import (
	"bufio"
	"bytes"
	"io"

	gzip "github.com/klauspost/compress/gzip"
)

// Reader store the bufio.Reader and gzip.Reader for a WARC file
type Reader struct {
	reader     *bufio.Reader
	gzipReader *gzip.Reader
	record     *Record
}

// Close closes the reader.
func (r *Reader) Close() {
	r.gzipReader.Close()
}
type reader interface {
	ReadString(delim byte) (line string, err error)
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

	// Parse the record content
	var tempBuf bytes.Buffer
	if _, err := tempBuf.ReadFrom(tempReader); err != nil {
		return nil, err
	}

	tempBuf.Truncate(bytes.LastIndex(tempBuf.Bytes(), []byte("\r\n\r\n")))

	// reading doesn't really need to be in TempDir, nor can we access it as it's on the client.
	buf := NewSpooledTempFile("warc", "", false)
	_, err = buf.Write(tempBuf.Bytes())
	if err != nil {
		return nil, err
	}

	r.record = &Record{
		Header:  header,
		Content: buf,
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
