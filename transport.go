package warc

import (
	"net/http"
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
