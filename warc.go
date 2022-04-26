/*
Package warc provides methods for reading and writing WARC files (https://iipc.github.io/warc-specifications/) in Go.
This module is based on nlevitt's WARC module (https://github.com/nlevitt/warc).
*/
package warc

import (
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

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChannel chan *RecordBatch, done chan bool, err error) {
	recordWriterChannel = make(chan *RecordBatch, 1)
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
	var currentWarcinfoRecordID string

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
	currentWarcinfoRecordID, err = warcWriter.WriteInfoRecord(settings.WarcinfoContent)
	if err != nil {
		panic(err)
	}

	// If compression is enabled, we close the record's GZIP chunk
	if settings.Compression != "" {
		if settings.Compression == "GZIP" {
			warcWriter.GZIPWriter.Close()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
			if err != nil {
				panic(err)
			}
		} else if settings.Compression == "ZSTD" {
			warcWriter.ZSTDWriter.Close()
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
				warcWriter.FileWriter.Flush()
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.GZIPWriter.Close()
					} else if settings.Compression == "ZSTD" {
						warcWriter.ZSTDWriter.Close()
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
				currentWarcinfoRecordID, err := warcWriter.WriteInfoRecord(settings.WarcinfoContent)
				if err != nil {
					panic(err)
				}
				_ = currentWarcinfoRecordID

				// If compression is enabled, we close the record's GZIP chunk
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.GZIPWriter.Close()
					} else if settings.Compression == "ZSTD" {
						warcWriter.ZSTDWriter.Close()
					}
				}
			}

			// Write all the records of the record batch
			for _, record := range recordBatch.Records {
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression)
				if err != nil {
					panic(err)
				}

				record.Header.Set("WARC-Date", recordBatch.CaptureTime)
				record.Header.Set("WARC-Warcinfo-ID", "<urn:uuid:"+currentWarcinfoRecordID+">")

				_, err := warcWriter.WriteRecord(record)
				if err != nil {
					panic(err)
				}

				// If compression is enabled, we close the record's GZIP chunk
				if settings.Compression != "" {
					if settings.Compression == "GZIP" {
						warcWriter.GZIPWriter.Close()
					} else if settings.Compression == "ZSTD" {
						warcWriter.ZSTDWriter.Close()
					}
				}
			}
			warcWriter.FileWriter.Flush()

			if recordBatch.Done != nil {
				recordBatch.Done <- true
			}
		} else {
			// Channel has been closed
			// We flush the data, close the file, and rename it
			warcWriter.FileWriter.Flush()
			if settings.Compression != "" {
				if settings.Compression == "GZIP" {
					warcWriter.GZIPWriter.Close()
				} else if settings.Compression == "ZSTD" {
					warcWriter.ZSTDWriter.Close()
				}
			}
			warcFile.Close()

			// The WARC file is renamed to remove the .open suffix
			err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
			if err != nil {
				println("Error renaming WARC file: ", err.Error())
				//This is most likely due to nothing being written to a WARC during a run, but could be a result of a larger issue.
				//panic(err)
			}

			done <- true

			return
		}
	}
}
