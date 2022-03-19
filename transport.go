package warc

import (
	"net/http"
)

type customTransport struct {
	http.Transport
	d *customDialer
	// h2t *http2.Transport
}

func (t *customTransport) RoundTrip(req *http.Request) (resp *http.Response, err error) {
	req.Header.Set("Accept-Encoding", "gzip")

	// conn, err := t.d.DialRequest(req)
	// if err != nil {
	// 	return nil, err
	// }

	// h2c, err := t.h2t.NewClientConn(conn)
	// if err != nil {
	// 	panic(err)
	// }

	resp, err = t.Transport.RoundTrip(req)
	if err != nil {
		return resp, err
	}

	return resp, nil
}
