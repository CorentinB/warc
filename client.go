package warc

import (
	"net/http"
	"sync"
)

type CustomHTTPClient struct {
	http.Client
	WARCWriter       chan *RecordBatch
	WARCWriterFinish chan bool
	WaitGroup        *sync.WaitGroup
	deduplication    dedupe_hash_table
	dedupe_options   dedupe_options
}

type dedupe_options struct {
	localDedupe bool
	CDXDedupe   bool
	CDXURL      string
}

type dedupe_hash_table struct {
	sync.RWMutex
	m map[string]revisitRecord
}

func (c *CustomHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	return nil
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string, decompressBody bool, dedupe_options dedupe_options) (httpClient *CustomHTTPClient, err error) {
	httpClient = new(CustomHTTPClient)

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupe_options = dedupe_options
	httpClient.deduplication.m = make(map[string]revisitRecord)

	// configure the waitgroup
	httpClient.WaitGroup = new(sync.WaitGroup)

	// configure WARC writer
	httpClient.WARCWriter, httpClient.WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		return nil, err
	}

	// configure HTTP client
	httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// configure custom dialer / transport
	customDialer := newCustomDialer(httpClient)
	customTransport, err := newCustomTransport(customDialer, proxy, decompressBody)
	if err != nil {
		return nil, err
	}

	httpClient.Transport = customTransport

	return httpClient, nil
}
