package warc

import (
	"net/http"
	"sync"
)

type customHTTPClient struct {
	http.Client
	WARCWriter       chan *RecordBatch
	WARCWriterFinish chan bool
	WaitGroup        *sync.WaitGroup
}

func (c *customHTTPClient) Close() error {
	c.WaitGroup.Wait()
	c.CloseIdleConnections()
	close(c.WARCWriter)
	<-c.WARCWriterFinish
	return nil
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string) (httpClient *customHTTPClient, err error) {
	httpClient = new(customHTTPClient)

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
	customTransport, err := newCustomTransport(customDialer, proxy)
	if err != nil {
		return nil, err
	}

	httpClient.Transport = customTransport

	return httpClient, nil
}
