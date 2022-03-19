package warc

import (
	"net/http"

	"golang.org/x/net/http2"
)

type customTransport struct {
	http.Transport
	d   *customDialer
	h2t *http2.Transport
}

func (t *customTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set("Accept-Encoding", "gzip")

	conn, err := t.d.DialRequest(req)
	if err != nil {
		return nil, err
	}

	h2c, err := t.h2t.NewClientConn(conn)
	if err != nil {
		panic(err)
		return nil, err
	}

	resp, err = h2c.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	return resp, nil
}
