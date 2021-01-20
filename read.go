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
// If onDisk is set to true, then the record's payload will be
// written to a temp file on disk, and specified in the *Record.PayloadPath,
// else, everything happen in memory.
func (r *Reader) ReadRecord(onDisk bool) (*Record, error) {
	var err error
	var tempReader *bufio.Reader

	r.gzipReader.Multistream(false)

	// If onDisk is specified, dump gzip block to a temporary file
	if onDisk {
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

		tempReader = bufio.NewReader(file)
	} else {
		tempReader = bufio.NewReader(r.gzipReader)
	}

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

	// If onDisk is specified, then we write the payload to a new temp file
	if onDisk {
		payloadTempFile, err := ioutil.TempFile("", "warc-reading-*")
		if err != nil {
			return nil, err
		}
		defer payloadTempFile.Close()

		// Copy all the payload (including the potential trailing CRLF)
		// to a newly created temporary file
		_, err = io.Copy(payloadTempFile, tempReader)
		if err != nil {
			payloadTempFile.Close()
			os.Remove(payloadTempFile.Name())
			return nil, err
		}

		// Check if the last 4 bytes are \r\n\r\n,
		// if yes, then we truncate the last 4 bytes
		buf := make([]byte, 16)
		stats, err := os.Stat(payloadTempFile.Name())
		if err != nil {
			payloadTempFile.Close()
			os.Remove(payloadTempFile.Name())
			return nil, err
		}

		start := stats.Size() - 16
		_, err = payloadTempFile.ReadAt(buf, start)
		if err != nil {
			payloadTempFile.Close()
			os.Remove(payloadTempFile.Name())
			return nil, err
		}

		if bytes.HasSuffix(buf, []byte("\r\n\r\n")) {
			os.Truncate(payloadTempFile.Name(), stats.Size()-4)
		}

		r.record = &Record{
			Header:      header,
			Content:     nil,
			PayloadPath: payloadTempFile.Name(),
		}
	} else {
		content, err := ioutil.ReadAll(tempReader)
		if err != nil {
			return nil, err
		}

		content = bytes.TrimSuffix(content, []byte("\r\n\r\n"))

		r.record = &Record{
			Header:  header,
			Content: bytes.NewReader(content),
		}
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
