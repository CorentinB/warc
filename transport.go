package warc

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"
)

type customTransport struct {
	t http.Transport
}

func (t *customTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req = req.Clone(req.Context())
	req.Header.Set("Accept-Encoding", "gzip")

	return t.t.RoundTrip(req)
}

func newCustomTransport(dialer *customDialer, proxy string) (t *customTransport, err error) {
	t = new(customTransport)

	t.t = http.Transport{
		// configure HTTP transport
		Dial:    dialer.CustomDial,
		DialTLS: dialer.CustomDialTLS,

		// disable keep alive
		MaxConnsPerHost:       0,
		IdleConnTimeout:       -1,
		TLSHandshakeTimeout:   15 * time.Second,
		ExpectContinueTimeout: 1 * time.Second,
		TLSNextProto:          make(map[string]func(authority string, c *tls.Conn) http.RoundTripper),
		TLSClientConfig: &tls.Config{
			InsecureSkipVerify: true,
		},
		DisableCompression:  true,
		ForceAttemptHTTP2:   false,
		MaxIdleConns:        -1,
		MaxIdleConnsPerHost: -1,
		DisableKeepAlives:   true,
	}

	// add proxy if specified
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, err
		}

		t.t.Proxy = http.ProxyURL(proxyURL)
	}

	return t, nil
}
