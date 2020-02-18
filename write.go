package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"strconv"
	"strings"
	"time"

	"github.com/klauspost/compress/zstd"
	uuid "github.com/satori/go.uuid"
)

// Writer writes WARC records to WARC files.
type Writer struct {
	FileName    string
	Compression string
	gzipWriter  *gzip.Writer
	zstdWriter  *zstd.Encoder
	fileWriter  *bufio.Writer
}

// RecordBatch is a structure that contains a bunch of
// records to be written at the same time, and a common
// capture timestamp
type RecordBatch struct {
	Records     []*Record
	CaptureTime string
}

// Record represents a WARC record.
type Record struct {
	Header  Header
	Content io.Reader
}

// WriteRecord writes a record to the underlying WARC file.
// A record consists of a version string, the record header followed by a
// record content block and two newlines:
// 	Version CLRF
// 	Header-Key: Header-Value CLRF
// 	CLRF
// 	Content
// 	CLRF
// 	CLRF
func (w *Writer) WriteRecord(r *Record) error {
	data, err := ioutil.ReadAll(r.Content)
	if err != nil {
		return err
	}

	// Add the mandatories headers
	r.Header["content-length"] = strconv.Itoa(len(data))

	if r.Header["warc-date"] == "" {
		r.Header["warc-date"] = time.Now().UTC().Format(time.RFC3339)
	}

	if r.Header["warc-type"] == "" {
		r.Header["warc-type"] = "resource"
	}

	if r.Header["warc-record-id"] == "" {
		r.Header["warc-record-id"] = "<urn:uuid:" + uuid.NewV4().String() + ">"
	}

	_, err = io.WriteString(w.fileWriter, "WARC/1.1\r\n")
	if err != nil {
		return err
	}

	// Write headers
	for key, value := range r.Header {
		_, err = io.WriteString(w.fileWriter, strings.Title(key)+": "+value+"\r\n")
		if err != nil {
			return err
		}
	}

	// Write payload
	_, err = io.WriteString(w.fileWriter, "\r\n"+string(data)+"\r\n\r\n")
	if err != nil {
		return err
	}

	// Flush data
	w.fileWriter.Flush()
	return nil
}

// WriteInfoRecord method can be used to write informations record to the WARC file
func (w *Writer) WriteInfoRecord(payload map[string]string) error {
	// Initialize the record
	infoRecord := NewRecord()

	// Set the headers
	infoRecord.Header.Set("WARC-Date", time.Now().UTC().Format(time.RFC3339))
	infoRecord.Header.Set("WARC-Filename", strings.TrimSuffix(w.FileName, ".open"))
	infoRecord.Header.Set("WARC-Type", "warcinfo")
	infoRecord.Header.Set("content-type", "application/warc-fields")

	// Write the payload
	warcInfoContent := new(bytes.Buffer)
	for k, v := range payload {
		warcInfoContent.WriteString(fmt.Sprintf("%s: %s\r\n", k, v))
	}
	infoRecord.Content = warcInfoContent

	// Finally, write the record and flush the data
	err := w.WriteRecord(infoRecord)
	if err != nil {
		return err
	}

	w.fileWriter.Flush()
	return nil
}
