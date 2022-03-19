package warc

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"sync"
	"time"

	"golang.org/x/net/http2"
)

var (
	// WARC writer related channels
	WARCWriter       chan *RecordBatch
	WARCWriterFinish chan bool

	// Custom HTTP clients
	HTTPClient        *http.Client
	ProxiedHTTPClient *http.Client

	WaitGroup *sync.WaitGroup
)

func init() {
	WaitGroup = new(sync.WaitGroup)
	HTTPClient = new(http.Client)
}

func Close() {
	HTTPClient.CloseIdleConnections()
	ProxiedHTTPClient.CloseIdleConnections()

	WARCWriterFinish <- true

	WaitGroup.Wait()
}

func NewWARCWritingHTTPClient(rotatorSettings *RotatorSettings, proxy string) (err error) {
	var (
		customTransport = new(customTransport)
		customDialer    = new(customDialer)
	)

	// configure net dialer
	customDialer.Timeout = 30 * time.Second
	customDialer.KeepAlive = -1

	// configure HTTP transport
	customTransport.Proxy = nil
	customTransport.MaxIdleConns = -1
	customTransport.IdleConnTimeout = -1
	customTransport.TLSHandshakeTimeout = 15 * time.Second
	customTransport.ExpectContinueTimeout = 1 * time.Second
	customTransport.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	customTransport.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}
	customTransport.d = customDialer
	// customTransport.Dial = customDialer.CustomDial
	// customTransport.DialTLS = customDialer.CustomDialTLS

	customTransport.DisableCompression = true
	customTransport.ForceAttemptHTTP2 = false

	HTTPClient.Transport = customTransport

	// configure HTTP client
	HTTPClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
		return http.ErrUseLastResponse
	}

	// configure HTTP2
	h2t, err := http2.ConfigureTransports(&customTransport.Transport)
	if err != nil {
		return err
	}
	customTransport.h2t = h2t

	// set our custom transport as our HTTP client transport
	HTTPClient.Transport = customTransport

	// init WARC rotator
	WARCWriter, WARCWriterFinish, err = rotatorSettings.NewWARCRotator()
	if err != nil {
		return err
	}

	// init the secondary HTTP client dedicated to requests that should
	// be executed through the specified proxy
	if proxy != "" {
		customProxiedHTTPTransport := customTransport
		ProxiedHTTPClient = HTTPClient

		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return err
		}

		customProxiedHTTPTransport.Proxy = http.ProxyURL(proxyURL)
		ProxiedHTTPClient.Transport = customProxiedHTTPTransport
	}

	return nil
}
