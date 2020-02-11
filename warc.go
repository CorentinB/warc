/*
Package warc provides methods for reading and writing WARC files (https://iipc.github.io/warc-specifications/) in Go.
This module is based on nlevitt's WARC module (https://github.com/nlevitt/warc).
*/
package warc

import (
	"context"
	"net/http"
	"net/http/httputil"
	"os"
	"strings"
	"sync/atomic"
)

var exitRequested int32

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
	// To use or to not use Gzip compression
	Encryption bool
	// WarcSize is in MegaBytes
	WarcSize float64
	// Directory where the created WARC files will be stored,
	// default will be the current directory
	OutputDirectory string
}

// ExchangeFromHTTPResponse turns a http.Response into a warc.Exchange
// filling both Response and Request records
func ExchangeFromHTTPResponse(response *http.Response) (*Exchange, error) {
	var exchange = NewExchange()

	// Dump response
	responseDump, err := httputil.DumpResponse(response, true)
	if err != nil {
		return exchange, err
	}

	// Add the response to the exchange
	exchange.Response.Header.Set("WARC-Type", "response")
	exchange.Response.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(responseDump))
	exchange.Response.Header.Set("WARC-Target-URI", response.Request.URL.String())
	exchange.Response.Header.Set("Content-Type", "application/http; msgtype=response")

	exchange.Response.Content = strings.NewReader(string(responseDump))

	// Dump request
	requestDump, err := httputil.DumpRequestOut(response.Request, true)
	if err != nil {
		return exchange, err
	}

	// Add the request to the exchange
	exchange.Request.Header.Set("WARC-Type", "request")
	exchange.Request.Header.Set("WARC-Payload-Digest", "sha1:"+GetSHA1(requestDump))
	exchange.Request.Header.Set("WARC-Target-URI", response.Request.URL.String())
	exchange.Request.Header.Set("Host", response.Request.URL.Host)
	exchange.Request.Header.Set("Content-Type", "application/http; msgtype=request")

	exchange.Request.Content = strings.NewReader(string(requestDump))

	return exchange, nil
}

// NewWARCRotator creates and return a channel that can be used
// to communicate records to be written to WARC files to the
// recordWriter function running in a goroutine
func (s *RotatorSettings) NewWARCRotator() (recordWriterChannel chan *Exchange, done chan bool, err error) {
	recordWriterChannel = make(chan *Exchange)
	done = make(chan bool)

	// Check the rotator settings, also set default values
	err = checkRotatorSettings(s)
	if err != nil {
		return recordWriterChannel, done, err
	}

	// Handle termination signals to properly close WARC files afterwards
	c, cancel := context.WithCancel(context.Background())
	go listenCtrlC(cancel)

	// Start the record writer in a goroutine
	// TODO: support for pool of recordWriter?
	go recordWriter(c, s, recordWriterChannel, done)

	return recordWriterChannel, done, nil
}

func recordWriter(c context.Context, settings *RotatorSettings, exchanges chan *Exchange, done chan bool) {
	var serial = 1
	var currentFileName string = generateWarcFileName(settings.Prefix, settings.Encryption, serial)

	// Create and open the initial file
	warcFile, err := os.Create(settings.OutputDirectory + currentFileName)
	if err != nil {
		panic(err)
	}

	// Initialize WARC writer
	warcWriter := NewWriter(warcFile, currentFileName, settings.Encryption)
	warcWriter.WriteInfoRecord(settings.WarcinfoContent)

	for {
		exchange, more := <-exchanges
		if more {
			if atomic.LoadInt32(&exitRequested) == 0 {
				if isFileSizeExceeded(settings.OutputDirectory+currentFileName, settings.WarcSize) {
					// WARC file size exceeded settings.WarcSize
					// The WARC file is renamed to remove the .open suffix
					err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
					if err != nil {
						panic(err)
					}

					// We flush the data and close the file
					warcWriter.fileWriter.Flush()
					if settings.Encryption {
						warcWriter.gzipWriter.Close()
					}
					warcFile.Close()

					// Increment the file's serial number, then create the new file
					serial++
					currentFileName = generateWarcFileName(settings.Prefix, settings.Encryption, serial)
					warcFile, err = os.Create(settings.OutputDirectory + currentFileName)
					if err != nil {
						panic(err)
					}

					// Initialize new WARC writer and write warcinfo record
					warcWriter = NewWriter(warcFile, currentFileName, settings.Encryption)
					warcWriter.WriteInfoRecord(settings.WarcinfoContent)
				}
				// Write response first, then the request
				exchange.Response.Header.Set("WARC-Date", exchange.CaptureTime)
				exchange.Response.Header.Set("WARC-Filename", strings.TrimSuffix(currentFileName, ".open"))
				err := warcWriter.WriteRecord(exchange.Response)
				if err != nil {
					panic(err)
				}

				exchange.Request.Header.Set("WARC-Date", exchange.CaptureTime)
				exchange.Request.Header.Set("WARC-Filename", strings.TrimSuffix(currentFileName, ".open"))
				err = warcWriter.WriteRecord(exchange.Request)
				if err != nil {
					panic(err)
				}
			} else {
				// Termination signal has been caught
				// We flush the data and close the file
				warcWriter.fileWriter.Flush()
				if settings.Encryption {
					warcWriter.gzipWriter.Close()
				}
				warcFile.Close()

				// The WARC file is renamed to remove the .open suffix
				err := os.Rename(settings.OutputDirectory+currentFileName, strings.TrimSuffix(settings.OutputDirectory+currentFileName, ".open"))
				if err != nil {
					panic(err)
				}

				done <- true

				os.Exit(130)
			}
		} else {
			warcWriter.fileWriter.Flush()
			if settings.Encryption {
				warcWriter.gzipWriter.Close()
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
