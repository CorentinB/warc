package warc

import (
	"bufio"
	"fmt"
	"io"
	"strconv"
	"strings"
	"time"

	gzip "github.com/klauspost/compress/gzip"

	"github.com/klauspost/compress/zstd"
	uuid "github.com/satori/go.uuid"
)

// Writer writes WARC records to WARC files.
type Writer struct {
	FileName    string
	Compression string
	GZIPWriter  *gzip.Writer
	ZSTDWriter  *zstd.Encoder
	FileWriter  *bufio.Writer
}

// RecordBatch is a structure that contains a bunch of
// records to be written at the same time, and a common
// capture timestamp
type RecordBatch struct {
	Records     []*Record
	Done        chan bool
	CaptureTime string
}

// Record represents a WARC record.
type Record struct {
	Header  Header
	Content ReadWriteSeekCloser
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
func (w *Writer) WriteRecord(r *Record) (recordID string, err error) {
	defer r.Content.Close()

	// Add the mandatories headers
	if r.Header.Get("WARC-Date") == "" {
		r.Header.Set("WARC-Date", time.Now().UTC().Format(time.RFC3339Nano))
	}

	if r.Header.Get("WARC-Type") == "" {
		r.Header.Set("WARC-Type", "resource")
	}

	if r.Header.Get("WARC-Record-ID") == "" {
		recordID = uuid.NewV4().String()
		r.Header.Set("WARC-Record-ID", "<urn:uuid:"+recordID+">")
	}

	if _, err := io.WriteString(w.FileWriter, "WARC/1.1\r\n"); err != nil {
		return recordID, err
	}

	// Write headers
	if r.Header.Get("Content-Length") == "" {
		r.Header.Set("Content-Length", strconv.Itoa(getContentLength(r.Content)))
	}

	if r.Header.Get("WARC-Block-Digest") == "" {
		r.Content.Seek(0, 0)
		r.Header.Set("WARC-Block-Digest", "sha1:"+GetSHA1(r.Content))
	}

	for key, value := range r.Header {
		if _, err = io.WriteString(w.FileWriter, strings.Title(key)+": "+value+"\r\n"); err != nil {
			return recordID, err
		}
	}

	if _, err := io.WriteString(w.FileWriter, "\r\n"); err != nil {
		return recordID, err
	}

	r.Content.Seek(0, 0)
	if _, err := io.Copy(w.FileWriter, r.Content); err != nil {
		return recordID, err
	}

	if _, err := io.WriteString(w.FileWriter, "\r\n\r\n"); err != nil {
		return recordID, err
	}

	// Flush data
	w.FileWriter.Flush()

	return recordID, nil
}

// WriteInfoRecord method can be used to write informations record to the WARC file
func (w *Writer) WriteInfoRecord(payload map[string]string) (recordID string, err error) {
	// Initialize the record
	infoRecord := NewRecord("", false)

	// Set the headers
	infoRecord.Header.Set("WARC-Date", time.Now().UTC().Format(time.RFC3339Nano))
	infoRecord.Header.Set("WARC-Filename", strings.TrimSuffix(w.FileName, ".open"))
	infoRecord.Header.Set("WARC-Type", "warcinfo")
	infoRecord.Header.Set("Content-Type", "application/warc-fields")

	// Write the payload
	for k, v := range payload {
		infoRecord.Content.Write([]byte(fmt.Sprintf("%s: %s\r\n", k, v)))
	}

	// Generate WARC-Block-Digest
	infoRecord.Header.Set("WARC-Block-Digest", "sha1:"+GetSHA1(infoRecord.Content))

	// Finally, write the record and flush the data
	recordID, err = w.WriteRecord(infoRecord)
	if err != nil {
		return recordID, err
	}

	w.FileWriter.Flush()

	return recordID, err
}
