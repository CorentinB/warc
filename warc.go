/*
Package warc provides methods for reading and writing WARC files (https://iipc.github.io/warc-specifications/) in Go.
This module is based on nlevitt's WARC module (https://github.com/nlevitt/warc).
*/
package warc

import (
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
)

// RotatorSettings is used to store the settings
// needed by recordWriter to write WARC files
type RotatorSettings struct {
	// Content of the warcinfo record that will be written
	// to all WARC files
	WarcinfoContent Header
	// Prefix used for WARC filenames, WARC 1.1 specifications
	// recommend to name files this way:
	// Prefix-Timestamp-Serial-Crawlhost.warc.gz
	Prefix string
	// Compression algorithm to use
	Compression string
	// WarcSize is in MegaBytes
	WarcSize float64
	// Directory where the created WARC files will be stored,
	// default will be the current directory
	OutputDirectory string
}

// RecordsFromHTTPResponse turns a http.Response into a warc.RecordBatch
// filling both Response and Request records
func RecordsFromHTTPResponse(response *http.Response) (*RecordBatch, error) {
	var batch = NewRecordBatch()

	// Dump response
	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		return batch, err
	}

	// Add the response to the exchange
	var responseRecord = NewRecord()
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(responseDump))
	responseRecord.Header.Set("WARC-Target-URI", response.Request.URL.String())
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	responseRecord.Content = strings.NewReader(string(responseDump))

	// Dump request
	requestDump, err := httputil.DumpRequestOut(response.Request, true)
	if err != nil {
		return batch, err
	}

	// Add the request to the exchange
	var requestRecord = NewRecord()
	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(requestDump))
	requestRecord.Header.Set("WARC-Target-URI", response.Request.URL.String())
	requestRecord.Header.Set("Host", response.Request.URL.Host)
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	requestRecord.Content = strings.NewReader(string(requestDump))

	// Append records to the record batch
	batch.Records = append(batch.Records, responseRecord, requestRecord)

	return batch, nil
}

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChannel chan *RecordBatch, done chan bool, err error) {
	recordWriterChannel = make(chan *RecordBatch)
	done = make(chan bool)

	// Check the rotator settings, also set default values
	err = checkRotatorSettings(s)
	if err != nil {
		return recordWriterChannel, done, err
	}

	// Start the record writer in a goroutine
	// TODO: support for pool of recordWriter?
	go recordWriter(s, recordWriterChannel, done)

	return recordWriterChannel, done, nil
}

func recordWriter(settings *RotatorSettings, records chan *RecordBatch, done chan bool) {
	var serial = 1
	var currentFileName string = generateWarcFileName(settings.Prefix, settings.Compression, serial)

	// Create and open the initial file
	warcFile, err := os.Create(settings.OutputDirectory + currentFileName)
	if err != nil {
		panic(err)
	}

	// Initialize WARC writer
	warcWriter, err := NewWriter(warcFile, currentFileName, settings.Compression)
	if err != nil {
		panic(err)
	}

	// Write the info record
	warcWriter.WriteInfoRecord(settings.WarcinfoContent)

	// If compression is enabled, we close the record's GZIP chunk
	if settings.Compression != "" {
		if settings.Compression == "GZIP" {
			warcWriter.gzipWriter.Close()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}
		} else if settings.Compression == "ZSTD" {
			warcWriter.zstdWriter.Close()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}
		}
	}

	for {
		recordBatch, more := <-records
		if more {
			if isFileSizeExceeded(settings.OutputDirectory+currentFileName, settings.WarcSize) {
				// WARC file size exceeded settings.WarcSize
				// The WARC file is renamed to remove the .open suffix
				err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
				if err != nil {
					panic(err)
				}

				// We flush the data and close the file
				warcWriter.fileWriter.Flush()
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.gzipWriter.Close()
					} else if settings.Compression == "ZSTD" {
						warcWriter.zstdWriter.Close()
					}
				}
				warcFile.Close()

				// Increment the file's serial number, then create the new file
				serial++
				currentFileName = generateWarcFileName(settings.Prefix, settings.Compression, serial)
				warcFile, err = os.Create(settings.OutputDirectory + currentFileName)
				if err != nil {
					panic(err)
				}

				// Initialize new WARC writer
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
				if err != nil {
					panic(err)
				}

				// Write the info record
				warcWriter.WriteInfoRecord(settings.WarcinfoContent)

				// If compression is enabled, we close the record's GZIP chunk
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.gzipWriter.Close()
						warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
						if err != nil {
							panic(err)
						}
					} else if settings.Compression == "ZSTD" {
						warcWriter.zstdWriter.Close()
						warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
						if err != nil {
							panic(err)
						}
					}
				}
			} else {
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
				if err != nil {
					panic(err)
				}
			}

			// Write all the records of the record batch
			for _, record := range recordBatch.Records {
				record.Header.Set("WARC-Date", recordBatch.CaptureTime)
				record.Header.Set("WARC-Filename", strings.TrimSuffix(currentFileName, ".open"))
				err := warcWriter.WriteRecord(record)
				if err != nil {
					panic(err)
				}

				// If compression is enabled, we close the record's GZIP chunk
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.gzipWriter.Close()
					} else if settings.Compression == "ZSTD" {
						warcWriter.zstdWriter.Close()
					}
				}
			}
		} else {
			// Channel has been closed
			// We flush the data, close the file, and rename it
			warcWriter.fileWriter.Flush()
			if settings.Compression != "" {
				if settings.Compression == "GZIP" {
					warcWriter.gzipWriter.Close()
				} else if settings.Compression == "ZSTD" {
					warcWriter.zstdWriter.Close()
				}
			}
			warcFile.Close()

			// The WARC file is renamed to remove the .open suffix
			err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
			if err != nil {
				panic(err)
			}

			done <- true

			return
		}
	}
}
