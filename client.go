package warc

import (
	"net/http"
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
	WARCTempDir         string
}

func (c *CustomHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	return nil
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string, decompressBody bool, dedupeOptions DedupeOptions, skipHTTPStatusCodes []int) (httpClient *CustomHTTPClient, err error) {
	httpClient = new(CustomHTTPClient)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = dedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Configure HTTP status code skipping (usually 429)
	httpClient.skipHTTPStatusCodes = skipHTTPStatusCodes

	// Configure WARC temporary file directory from RotatorSettings.
	httpClient.WARCTempDir = path.Dir(rotatorSettings.OutputDirectory) + "/temp"

	// Configure the waitgroup
	httpClient.WaitGroup = new(sync.WaitGroup)

	// Configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, err
	}

	// Configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// Configure custom dialer / transport
	customDialer := newCustomDialer(httpClient)
	customTransport, err := newCustomTransport(customDialer, proxy, decompressBody)
	if err != nil {
		return nil, err
	}

	httpClient.Transport = customTransport

	return httpClient, nil
}
