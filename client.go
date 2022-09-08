package warc

import (
	"net/http"
	"os"
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
	WARCWriter             chan *RecordBatch
	WARCWriterDoneChannels []chan bool
	WaitGroup              *WaitGroupWithCount
	dedupeHashTable        *sync.Map
	dedupeOptions          DedupeOptions
	skipHTTPStatusCodes    []int
	errChan                chan error
	verifyCerts            bool
	TempDir                string
	FullOnDisk             bool
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
	if HTTPClientSettings.TempDir != "" {
		httpClient.TempDir = HTTPClientSettings.TempDir
		err = os.MkdirAll(httpClient.TempDir, os.ModePerm)
		if err != nil {
			return nil, errChan, err
		}
	}

	// Configure if we are only storing responses only on disk or in memory and on disk.
	httpClient.FullOnDisk = HTTPClientSettings.FullOnDisk

	// Configure the waitgroup
	httpClient.WaitGroup = new(WaitGroupWithCount)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterDoneChannels, err = HTTPClientSettings.RotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, errChan, err
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer, err := newCustomDialer(httpClient, HTTPClientSettings.Proxy)
	if err != nil {
		return nil, errChan, err
	}

	customTransport, err := newCustomTransport(customDialer, HTTPClientSettings.DecompressBody)
	if err != nil {
		return nil, errChan, err
	}

	httpClient.Transport = customTransport

	return httpClient, errChan, nil
}
