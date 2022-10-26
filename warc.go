package warc

import (
	"errors"
	"os"
	"strings"
	"sync"

	"github.com/paulbellamy/ratecounter"
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
	// WARCWriterPoolSize defines the number of parallel WARC writers
	WARCWriterPoolSize int
}

var (
	// Create mutex to ensure we are generating WARC files one at a time and not naming them the same thing.
	fileMutex sync.Mutex

	// Create a counter to keep track of the number of bytes written to WARC files
	DataTotal *ratecounter.Counter
)

func init() {
	// Initialize the counters
	DataTotal = new(ratecounter.Counter)
}

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChan chan *RecordBatch, doneChannels []chan bool, err error) {
	recordWriterChan = make(chan *RecordBatch, 1)

	// Check the rotator settings and set default values
	err = checkRotatorSettings(s)
	if err != nil {
		return recordWriterChan, doneChannels, err
	}

	for i := 0; i < s.WARCWriterPoolSize; i++ {
		doneChan := make(chan bool)
		doneChannels = append(doneChannels, doneChan)

		go recordWriter(s, recordWriterChan, doneChan)
	}

	return recordWriterChan, doneChannels, nil
}

func (w *Writer) CloseCompressedWriter() {
	if w.GZIPWriter != nil {
		w.GZIPWriter.Close()
	} else if w.PGZIPWriter != nil {
		w.PGZIPWriter.Close()
	} else if w.ZSTDWriter != nil {
		w.ZSTDWriter.Close()
	}
}

func recordWriter(settings *RotatorSettings, records chan *RecordBatch, done chan bool) {
	var (
		serial                  = 1
		currentFileName         = generateWarcFileName(settings.Prefix, settings.Compression, serial)
		currentWarcinfoRecordID string
	)

	// Ensure file doesn't already exist (and if it does, make a new one)
	fileMutex.Lock()
	_, err := os.Stat(settings.OutputDirectory + currentFileName)
	for !errors.Is(err, os.ErrNotExist) {
		currentFileName = generateWarcFileName(settings.Prefix, settings.Compression, serial)
		_, err = os.Stat(settings.OutputDirectory + currentFileName)
	}

	// Create and open the initial file
	warcFile, err := os.Create(settings.OutputDirectory + currentFileName)
	if err != nil {
		panic(err)
	}
	fileMutex.Unlock()

	// Initialize WARC writer
	warcWriter, err := NewWriter(warcFile, currentFileName, settings.Compression, "")
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
			warcWriter.CloseCompressedWriter()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "")
			if err != nil {
				panic(err)
			}
		} else if settings.Compression == "ZSTD" {
			warcWriter.CloseCompressedWriter()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "")
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
					warcWriter.CloseCompressedWriter()
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
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "")
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
					warcWriter.CloseCompressedWriter()
				}
			}

			// Write all the records of the record batch
			for _, record := range recordBatch.Records {
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, record.Header.Get("Content-Length"))
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
					warcWriter.CloseCompressedWriter()
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
				warcWriter.CloseCompressedWriter()
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
