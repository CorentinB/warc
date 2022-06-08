package warc

import (
	"compress/gzip"
	"crypto/tls"
	"log"
	"net/http"
	"net/http/httptrace"
	"net/url"
	"time"
)

type customTransport struct {
	t              http.Transport
	decompressBody bool
}

func (t *customTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req = req.Clone(req.Context())
	req.Header.Set("Accept-Encoding", "gzip")

	// Use httptrace to increment the URI/s counter on DNS requests.
	trace := &httptrace.ClientTrace{
		GotConn: func(connInfo httptrace.GotConnInfo) {
			log.Printf("got conn: %+v\n", connInfo)
		},
		DNSStart: func(info httptrace.DNSStartInfo) {
			t := time.Now().UTC().String()
			log.Println(t, "dns start")
			log.Println("dns host:", info.Host)
		},
		DNSDone: func(info httptrace.DNSDoneInfo) {
			t := time.Now().UTC().String()
			log.Println(t, "dns end")
			log.Println("addrs:", info.Addrs)
			log.Println("errs:", info.Err)
		},
	}

	req = req.WithContext(httptrace.WithClientTrace(req.Context(), trace))

	resp, err = t.t.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	// if the client have been created with decompressBody = true,
	// we decompress the resp.Body if we received a compressed body
	if t.decompressBody {
		switch resp.Header.Get("Content-Encoding") {
		case "gzip":
			resp.Body, err = gzip.NewReader(resp.Body)
		}
	}

	return
}

func newCustomTransport(dialer *customDialer, proxy string, decompressBody bool) (t *customTransport, err error) {
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

	t.decompressBody = decompressBody

	return t, nil
}
