package warc

import (
	"errors"
	"os"
	"path"
	"strings"
	"sync"

	"github.com/paulbellamy/ratecounter"
)

// RotatorSettings is used to store the settings
// needed by recordWriter to write WARC files
type RotatorSettings struct {
	WarcinfoContent       Header
	Prefix                string
	Compression           string
	CompressionDictionary string
	OutputDirectory       string
	WarcSize              float64
	WARCWriterPoolSize    int
}

var (
	// Create mutex to ensure we are generating WARC files one at a time and not naming them the same thing.
	fileMutex sync.Mutex

	// Create a counter to keep track of the number of bytes written to WARC files
	// and the number of bytes deduped
	DataTotal         *ratecounter.Counter
	RemoteDedupeTotal *ratecounter.Counter
	LocalDedupeTotal  *ratecounter.Counter
)

func init() {
	// Initialize the counters
	DataTotal = new(ratecounter.Counter)
	RemoteDedupeTotal = new(ratecounter.Counter)
	LocalDedupeTotal = new(ratecounter.Counter)
}

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChan chan *RecordBatch, doneChannels []chan bool, err error) {
	recordWriterChan = make(chan *RecordBatch, 1)
	// Create global atomicSerial number for numbering WARC files.
	var atomicSerial int64

	// Check the rotator settings and set default values
	err = checkRotatorSettings(s)
	if err != nil {
		return recordWriterChan, doneChannels, err
	}

	for i := 0; i < s.WARCWriterPoolSize; i++ {
		doneChan := make(chan bool)
		doneChannels = append(doneChannels, doneChan)

		go recordWriter(s, recordWriterChan, doneChan, &atomicSerial)
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

func recordWriter(settings *RotatorSettings, records chan *RecordBatch, done chan bool, atomicSerial *int64) {
	var (
		currentFileName         = GenerateWarcFileName(settings.Prefix, settings.Compression, atomicSerial)
		currentWarcinfoRecordID string
	)

	// Ensure file doesn't already exist (and if it does, make a new one)
	fileMutex.Lock()
	_, err := os.Stat(settings.OutputDirectory + currentFileName)
	for !errors.Is(err, os.ErrNotExist) {
		currentFileName = GenerateWarcFileName(settings.Prefix, settings.Compression, atomicSerial)
		_, err = os.Stat(settings.OutputDirectory + currentFileName)
	}

	// Create and open the initial file
	warcFile, err := os.Create(settings.OutputDirectory + currentFileName)
	if err != nil {
		panic(err)
	}
	fileMutex.Unlock()

	var dictionary []byte

	if settings.CompressionDictionary != "" {
		dictionary, err = os.ReadFile(settings.CompressionDictionary)
		if err != nil {
			panic(err)
		}
	}

	// Initialize WARC writer
	warcWriter, err := NewWriter(warcFile, currentFileName, settings.Compression, "", true, dictionary)
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
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "", false, dictionary)
			if err != nil {
				panic(err)
			}
		} else if settings.Compression == "ZSTD" {
			warcWriter.CloseCompressedWriter()
			warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "", false, dictionary)
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
				err := os.Rename(path.Join(settings.OutputDirectory, currentFileName), strings.TrimSuffix(path.Join(settings.OutputDirectory, currentFileName), ".open"))
				if err != nil {
					panic(err)
				}

				// We flush the data and close the file
				warcWriter.FileWriter.Flush()
				if settings.Compression != "" {
					warcWriter.CloseCompressedWriter()
				}
				warcFile.Close()

				// Create the new file and automatically increment the serial inside of GenerateWarcFileName
				currentFileName = GenerateWarcFileName(settings.Prefix, settings.Compression, atomicSerial)
				warcFile, err = os.Create(settings.OutputDirectory + currentFileName)
				if err != nil {
					panic(err)
				}

				// Initialize new WARC writer
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, "", true, dictionary)
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
				warcWriter, err = NewWriter(warcFile, currentFileName, settings.Compression, record.Header.Get("Content-Length"), false, dictionary)
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
