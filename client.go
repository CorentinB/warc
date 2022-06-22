package warc

import (
	"net/http"
	"sync"
)

type CustomHTTPClient struct {
	http.Client
	debug               bool
	WARCWriter          chan *RecordBatch
	WARCWriterFinish    chan bool
	errChan             chan error
	WaitGroup           *sync.WaitGroup
	dedupeHashTable     *sync.Map
	dedupeOptions       DedupeOptions
	skipHTTPStatusCodes []int
}

func (c *CustomHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	close(c.errChan)
	return nil
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string, decompressBody bool, dedupeOptions DedupeOptions, skipHTTPStatusCodes []int, debug bool) (*CustomHTTPClient, chan error, error) {
	var (
		httpClient = new(CustomHTTPClient)
		err        error
	)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = dedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Configure HTTP status code skipping (usually 429)
	httpClient.skipHTTPStatusCodes = skipHTTPStatusCodes

	// Create an error channel for sending WARC errors through
	httpClient.errChan = make(chan error)

	// Configure the waitgroup
	httpClient.WaitGroup = new(sync.WaitGroup)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, httpClient.errChan, err
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer := newCustomDialer(httpClient)
	customTransport, err := newCustomTransport(customDialer, proxy, decompressBody)
	if err != nil {
		return nil, httpClient.errChan, err
	}

	httpClient.Transport = customTransport

	return httpClient, httpClient.errChan, nil
}
