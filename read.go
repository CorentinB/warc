package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"io"
	"io/ioutil"
	"os"
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
func (r *Reader) ReadRecord() (*Record, error) {
	r.gzipReader.Multistream(false)

	// Dump gzip block to a temporary file
	tempFile, err := ioutil.TempFile("", "warc-reading-*")
	if err != nil {
		return nil, err
	}
	defer os.Remove(tempFile.Name())

	if _, err := io.Copy(tempFile, r.gzipReader); err != nil {
		tempFile.Close()
		return nil, err
	}
	tempFile.Close()

	// Open temp file and start parsing it
	file, err := os.Open(tempFile.Name())
	if err != nil {
		return nil, err
	}
	defer file.Close()

	tempReader := bufio.NewReader(file)

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

	content, err := ioutil.ReadAll(tempReader)
	if err != nil {
		return nil, err
	}

	content = bytes.TrimSuffix(content, []byte("\r\n\r\n"))

	r.record = &Record{
		Header:  header,
		Content: bytes.NewReader(content),
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
