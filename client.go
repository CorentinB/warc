package warc

import (
	"net/http"
	"os"
	"sync"

	"github.com/paulbellamy/ratecounter"
)

type Error struct {
	Err  error
	Func string
}

type HTTPClientSettings struct {
	RotatorSettings       *RotatorSettings
	DedupeOptions         DedupeOptions
	Proxy                 string
	DecompressBody        bool
	SkipHTTPStatusCodes   []int
	VerifyCerts           bool
	TempDir               string
	FullOnDisk            bool
	MaxReadBeforeTruncate int
}

type CustomHTTPClient struct {
	http.Client
	WARCWriter             chan *RecordBatch
	WARCWriterDoneChannels []chan bool
	WaitGroup              *WaitGroupWithCount
	dedupeHashTable        *sync.Map
	dedupeOptions          DedupeOptions
	skipHTTPStatusCodes    []int
	ErrChan                chan *Error
	verifyCerts            bool
	TempDir                string
	FullOnDisk             bool
	MaxReadBeforeTruncate  int
	DataTotal              *ratecounter.Counter
}

func (c *CustomHTTPClient) Close() error {
	var wg sync.WaitGroup
	c.WaitGroup.Wait()
	c.CloseIdleConnections()

	close(c.WARCWriter)

	wg.Add(len(c.WARCWriterDoneChannels))
	for _, doneChan := range c.WARCWriterDoneChannels {
		go func(done chan bool) {
			defer wg.Done()
			<-done
		}(doneChan)
	}

	wg.Wait()
	close(c.ErrChan)

	return nil
}

func NewWARCWritingHTTPClient(HTTPClientSettings HTTPClientSettings) (httpClient *CustomHTTPClient, err error) {
	httpClient = new(CustomHTTPClient)

	// Init data counters
	httpClient.DataTotal = new(ratecounter.Counter)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = HTTPClientSettings.DedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Configure HTTP status code skipping (usually 429)
	httpClient.skipHTTPStatusCodes = HTTPClientSettings.SkipHTTPStatusCodes

	// Create an error channel for sending WARC errors through
	httpClient.ErrChan = make(chan *Error)

	// Toggle verification of certificates
	// InsecureSkipVerify expects the opposite of the verifyCerts flag, as such we flip it.
	httpClient.verifyCerts = !HTTPClientSettings.VerifyCerts

	// Configure WARC temporary file directory
	if HTTPClientSettings.TempDir != "" {
		httpClient.TempDir = HTTPClientSettings.TempDir
		err = os.MkdirAll(httpClient.TempDir, os.ModePerm)
		if err != nil {
			return nil, err
		}
	}

	// Configure if we are only storing responses only on disk or in memory and on disk.
	httpClient.FullOnDisk = HTTPClientSettings.FullOnDisk

	// Configure our max read before we start truncating records
	if HTTPClientSettings.MaxReadBeforeTruncate == 0 {
		httpClient.MaxReadBeforeTruncate = 1000000000
	} else {
		httpClient.MaxReadBeforeTruncate = HTTPClientSettings.MaxReadBeforeTruncate
	}

	// Configure the waitgroup
	httpClient.WaitGroup = new(WaitGroupWithCount)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterDoneChannels, err = HTTPClientSettings.RotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, err
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer, err := newCustomDialer(httpClient, HTTPClientSettings.Proxy)
	if err != nil {
		return nil, err
	}

	customTransport, err := newCustomTransport(customDialer, HTTPClientSettings.DecompressBody)
	if err != nil {
		return nil, err
	}

	httpClient.Transport = customTransport

	return httpClient, nil
}
