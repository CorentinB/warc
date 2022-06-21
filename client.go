package warc

import (
	"net/http"
	"os"
	"path"
	"sync"
)

type CustomHTTPClient struct {
	http.Client
	WARCWriter          chan *RecordBatch
	WARCWriterFinish    chan bool
	WaitGroup           *sync.WaitGroup
	dedupeHashTable     *sync.Map
	dedupeOptions       DedupeOptions
	skipHTTPStatusCodes []int
	errChan             chan error
	WARCTempDir         string
}

func (c *CustomHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	close(c.errChan)
	return nil
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string, decompressBody bool, dedupeOptions DedupeOptions, skipHTTPStatusCodes []int) (httpClient *CustomHTTPClient, err error, errChan chan error) {
	httpClient = new(CustomHTTPClient)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = dedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Configure HTTP status code skipping (usually 429)
	httpClient.skipHTTPStatusCodes = skipHTTPStatusCodes

	// Create an error channel for sending WARC errors through
	errChan = make(chan error)
	httpClient.errChan = errChan

	// Configure WARC temporary file directory from RotatorSettings.
	if path.Dir(rotatorSettings.OutputDirectory) == "." {
		// if, for example, like in the tests we are using a single path like "warcs", we should use an upper directory, like temp/
		httpClient.WARCTempDir = "temp/"
	} else {
		httpClient.WARCTempDir = path.Join(rotatorSettings.OutputDirectory, "temp")
	}

	// Ensure the folder we are trying to write to, exists.
	if _, err := os.Stat(httpClient.WARCTempDir); os.IsNotExist(err) {
		os.MkdirAll(httpClient.WARCTempDir, os.ModePerm)
	}

	// Configure the waitgroup
	httpClient.WaitGroup = new(sync.WaitGroup)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, err, errChan
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer := newCustomDialer(httpClient)
	customTransport, err := newCustomTransport(customDialer, proxy, decompressBody)
	if err != nil {
		return nil, err, errChan
	}

	httpClient.Transport = customTransport

	return httpClient, nil, errChan
}
