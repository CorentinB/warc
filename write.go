package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"io/ioutil"
	"os"
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
	Done        chan bool
	CaptureTime string
}

// Record represents a WARC record.
type Record struct {
	Header      Header
	Content     io.Reader
	PayloadPath string
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
	// Generate record ID
	recordID = uuid.NewV4().String()

	// Add the mandatories headers
	if r.Header["warc-date"] == "" {
		r.Header["warc-date"] = time.Now().UTC().Format(time.RFC3339)
	}

	if r.Header["warc-type"] == "" {
		r.Header["warc-type"] = "resource"
	}

	if r.Header["warc-record-id"] == "" {
		r.Header["warc-record-id"] = "<urn:uuid:" + recordID + ">"
	}

	_, err = io.WriteString(w.fileWriter, "WARC/1.0\r\n")
	if err != nil {
		return recordID, err
	}

	// If PayloadPath isn't empty, it means that the payload we need to write
	// lives on disk
	if r.PayloadPath != "" {
		file, err := os.Open(r.PayloadPath)
		if err != nil {
			return recordID, err
		}
		defer file.Close()

		// Write headers
		fileStats, err := file.Stat()
		if err != nil {
			return recordID, err
		}
		r.Header["content-length"] = strconv.Itoa(int(fileStats.Size()))

		// Write headers
		for key, value := range r.Header {
			_, err = io.WriteString(w.fileWriter, strings.Title(key)+": "+value+"\r\n")
			if err != nil {
				return recordID, err
			}
		}

		_, err = io.WriteString(w.fileWriter, "\r\n")
		if err != nil {
			return recordID, err
		}

		bufferedReader := bufio.NewReader(file)
		buffer := make([]byte, 1024)
		for {
			count, err := bufferedReader.Read(buffer)
			if err != nil && err != io.EOF {
				return recordID, err
			}

			_, err = io.WriteString(w.fileWriter, string(buffer))
			if err != nil {
				return recordID, err
			}

			if count == 0 || err == io.EOF {
				break
			}
		}
	} else {
		data, err := ioutil.ReadAll(r.Content)
		if err != nil {
			return recordID, err
		}

		// Write headers
		r.Header["content-length"] = strconv.Itoa(len(data))
		for key, value := range r.Header {
			_, err = io.WriteString(w.fileWriter, strings.Title(key)+": "+value+"\r\n")
			if err != nil {
				return recordID, err
			}
		}

		_, err = io.WriteString(w.fileWriter, "\r\n"+string(data))
		if err != nil {
			return recordID, err
		}
	}

	_, err = io.WriteString(w.fileWriter, "\r\n\r\n")
	if err != nil {
		return recordID, err
	}

	// Flush data
	w.fileWriter.Flush()

	return recordID, nil
}

// WriteInfoRecord method can be used to write informations record to the WARC file
func (w *Writer) WriteInfoRecord(payload map[string]string) (recordID string, err error) {
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
	recordID, err = w.WriteRecord(infoRecord)
	if err != nil {
		return recordID, err
	}

	w.fileWriter.Flush()
	return recordID, err
}
