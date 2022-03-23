package warc

import (
	"crypto/tls"
	"net/http"
	"net/url"
	"time"
)

type customTransport struct {
	http.Transport
	d *customDialer
}

func (t *customTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set("Accept-Encoding", "gzip")

	resp, err = t.Transport.RoundTrip(req)
	if err != nil {
		panic(err)
	}

	return resp, nil
}

func newCustomTransport(dialer *customDialer, proxy string) (t *customTransport, err error) {
	t = new(customTransport)

	t.d = dialer
	t.Dial = dialer.CustomDial
	t.DialTLS = dialer.CustomDialTLS

	// configure HTTP transport
	t.MaxConnsPerHost = 0
	t.IdleConnTimeout = -1
	t.TLSHandshakeTimeout = 15 * time.Second
	t.ExpectContinueTimeout = 1 * time.Second
	t.TLSNextProto = make(map[string]func(authority string, c *tls.Conn) http.RoundTripper)
	t.TLSClientConfig = &tls.Config{
		InsecureSkipVerify: true,
	}

	t.DisableCompression = true
	t.ForceAttemptHTTP2 = false

	// disable keep alive
	t.MaxIdleConns = -1
	t.MaxIdleConnsPerHost = -1
	t.DisableKeepAlives = true

	// add proxy if specified
	if proxy != "" {
		proxyURL, err := url.Parse(proxy)
		if err != nil {
			return nil, err
		}

		t.Proxy = http.ProxyURL(proxyURL)
	}

	return t, nil
}
