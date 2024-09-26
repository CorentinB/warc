package warc

import (
	"net/http"
	"os"
	"sync"
	"time"
)

type Error struct {
	Err  error
	Func string
}

type HTTPClientSettings struct {
	RotatorSettings       *RotatorSettings
	Proxy                 string
	TempDir               string
	DNSServer             string
	SkipHTTPStatusCodes   []int
	DNSServers            []string
	DedupeOptions         DedupeOptions
	DialTimeout           time.Duration
	ResponseHeaderTimeout time.Duration
	DNSResolutionTimeout  time.Duration
	DNSRecordsTTL         time.Duration
	TLSHandshakeTimeout   time.Duration
	TCPTimeout            time.Duration
	MaxReadBeforeTruncate int
	DecompressBody        bool
	FollowRedirects       bool
	FullOnDisk            bool
	VerifyCerts           bool
	RandomLocalIP         bool
	DisableIPv4           bool
	DisableIPv6           bool
	IPv6AnyIP             bool
}

type CustomHTTPClient struct {
	interfacesWatcherStop    chan bool
	WaitGroup                *WaitGroupWithCount
	dedupeHashTable          *sync.Map
	ErrChan                  chan *Error
	WARCWriter               chan *RecordBatch
	interfacesWatcherStarted chan bool
	http.Client
	TempDir                string
	WARCWriterDoneChannels []chan bool
	skipHTTPStatusCodes    []int
	dedupeOptions          DedupeOptions
	TLSHandshakeTimeout    time.Duration
	MaxReadBeforeTruncate  int
	verifyCerts            bool
	FullOnDisk             bool
	randomLocalIP          bool
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

	if c.randomLocalIP {
		c.interfacesWatcherStop <- true
		close(c.interfacesWatcherStop)
	}

	return nil
}

func NewWARCWritingHTTPClient(HTTPClientSettings HTTPClientSettings) (httpClient *CustomHTTPClient, err error) {
	httpClient = new(CustomHTTPClient)

	// Configure random local IP
	httpClient.randomLocalIP = HTTPClientSettings.RandomLocalIP
	if httpClient.randomLocalIP {
		httpClient.interfacesWatcherStop = make(chan bool)
		httpClient.interfacesWatcherStarted = make(chan bool)
		go httpClient.getAvailableIPs(HTTPClientSettings.IPv6AnyIP)
		<-httpClient.interfacesWatcherStarted
	}

	// Toggle deduplication options and create map for deduplication records.
	httpClient.dedupeOptions = HTTPClientSettings.DedupeOptions
	httpClient.dedupeHashTable = new(sync.Map)

	// Set default deduplication threshold to 1024 bytes
	if httpClient.dedupeOptions.SizeThreshold == 0 {
		httpClient.dedupeOptions.SizeThreshold = 1024
	}

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
	if !HTTPClientSettings.FollowRedirects {
		httpClient.CheckRedirect = func(req *http.Request, via []*http.Request) error {
			return http.ErrUseLastResponse
		}
	}

	// Verify timeouts and set default values
	if HTTPClientSettings.DialTimeout == 0 {
		HTTPClientSettings.DialTimeout = 10 * time.Second
	}

	if HTTPClientSettings.ResponseHeaderTimeout == 0 {
		HTTPClientSettings.ResponseHeaderTimeout = 10 * time.Second
	}

	if HTTPClientSettings.TLSHandshakeTimeout == 0 {
		HTTPClientSettings.TLSHandshakeTimeout = 10 * time.Second
	}

	if HTTPClientSettings.TCPTimeout == 0 {
		HTTPClientSettings.TCPTimeout = 10 * time.Second
	}

	if HTTPClientSettings.DNSResolutionTimeout == 0 {
		HTTPClientSettings.DNSResolutionTimeout = 5 * time.Second
	}

	if HTTPClientSettings.DNSRecordsTTL == 0 {
		HTTPClientSettings.DNSRecordsTTL = 5 * time.Minute
	}

	httpClient.TLSHandshakeTimeout = HTTPClientSettings.TLSHandshakeTimeout

	// Configure custom dialer / transport
	customDialer, err := newCustomDialer(httpClient, HTTPClientSettings.Proxy, HTTPClientSettings.DialTimeout, HTTPClientSettings.DNSRecordsTTL, HTTPClientSettings.DNSResolutionTimeout, HTTPClientSettings.DNSServers, HTTPClientSettings.DisableIPv4, HTTPClientSettings.DisableIPv6)
	if err != nil {
		return nil, err
	}

	customTransport, err := newCustomTransport(customDialer, HTTPClientSettings.DecompressBody, HTTPClientSettings.TLSHandshakeTimeout)
	if err != nil {
		return nil, err
	}

	httpClient.Transport = customTransport

	return httpClient, nil
}
