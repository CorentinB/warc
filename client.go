package warc

import (
	"net/http"
	"sync"
)

type HTTPClientSettings struct {
	RotatorSettings     *RotatorSettings
	DedupeOptions       DedupeOptions
	Proxy               string
	DecompressBody      bool
	SkipHTTPStatusCodes []int
	VerifyCerts         bool
	TempDir             string
	FullOnDisk          bool
}

type CustomHTTPClient struct {
	http.Client
	WARCWriter          chan *RecordBatch
	WARCWriterFinish    chan bool
	WaitGroup           *sync.WaitGroup
	dedupeHashTable     *sync.Map
	dedupeOptions       DedupeOptions
	skipHTTPStatusCodes []int
	errChan             chan error
	verifyCerts         bool
	TempDir             string
	FullOnDisk          bool
}

func (c *CustomHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	close(c.errChan)
	return nil
}

func NewWARCWritingHTTPClient(HTTPClientSettings HTTPClientSettings) (httpClient *CustomHTTPClient, errChan chan error, err error) {
	httpClient = new(CustomHTTPClient)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = HTTPClientSettings.DedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Configure HTTP status code skipping (usually 429)
	httpClient.skipHTTPStatusCodes = HTTPClientSettings.SkipHTTPStatusCodes

	// Create an error channel for sending WARC errors through
	errChan = make(chan error)
	httpClient.errChan = errChan

	// Toggle verification of certificates
	// InsecureSkipVerify expects the opposite of the verifyCerts flag, as such we flip it.
	httpClient.verifyCerts = !HTTPClientSettings.VerifyCerts

	// Configure WARC temporary file directory
	httpClient.TempDir = HTTPClientSettings.TempDir

	// Configure if we are only storing responses only on disk or in memory and on disk.
	httpClient.FullOnDisk = HTTPClientSettings.FullOnDisk

	// Configure the waitgroup
	httpClient.WaitGroup = new(sync.WaitGroup)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterFinish, err = HTTPClientSettings.RotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, errChan, err
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer := newCustomDialer(httpClient)
	customTransport, err := newCustomTransport(customDialer, HTTPClientSettings.Proxy, HTTPClientSettings.DecompressBody)
	if err != nil {
		return nil, errChan, err
	}

	httpClient.Transport = customTransport

	return httpClient, errChan, nil
}
