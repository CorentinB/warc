package warc

import (
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"

	"github.com/remeh/sizedwaitgroup"
)

type customDialer struct {
	net.Dialer
}

type customConnection struct {
	net.Conn
	io.Reader
	io.Writer
	closers []io.Closer
}

func (cc *customConnection) Read(b []byte) (int, error) {
	return cc.Reader.Read(b)
}

func (cc *customConnection) Write(b []byte) (int, error) {
	return cc.Writer.Write(b)
}

func (cc *customConnection) Close() error {
	for _, c := range cc.closers {
		c.Close()
	}

	return cc.Conn.Close()
}

func wrapConnection(c net.Conn, URL *url.URL) net.Conn {
	reqReader, reqWriter := io.Pipe()
	respReader, respWriter := io.Pipe()

	WaitGroup.Add(1)
	go writeWARCFromConnection(reqReader, respReader, URL)

	return &customConnection{
		Conn:    c,
		closers: []io.Closer{reqWriter, respWriter},
		Reader:  io.TeeReader(c, respWriter),
		Writer:  io.MultiWriter(c, reqWriter),
	}
}

func (dialer *customDialer) DialRequest(req *http.Request) (net.Conn, error) {
	switch req.URL.Scheme {
	case "http":
		return dialer.CustomDialWithURL("tcp", req.Host+":80", req.URL)
	case "https":
		return dialer.CustomDialTLSWithURL("tcp", req.Host+":443", req.URL)
	default:
		panic("WTF?!?")
	}
}

func (dialer *customDialer) CustomDial(network, address string) (net.Conn, error) {
	u, _ := url.Parse("http://" + address)
	return dialer.CustomDialWithURL(network, address, u)
}

func (dialer *customDialer) CustomDialTLS(network, address string) (net.Conn, error) {
	u, _ := url.Parse("https://" + address)
	return dialer.CustomDialTLSWithURL(network, address, u)
}

func (dialer *customDialer) CustomDialWithURL(network, address string, URL *url.URL) (net.Conn, error) {
	conn, err := dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return wrapConnection(conn, URL), nil
}

func (dialer *customDialer) CustomDialTLSWithURL(network, address string, URL *url.URL) (net.Conn, error) {
	plainConn, err := dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}

	cfg := new(tls.Config)
	serverName := address[:strings.LastIndex(address, ":")]
	cfg.ServerName = serverName

	tlsConn := tls.Client(plainConn, cfg)

	errc := make(chan error, 2)
	timer := time.AfterFunc(time.Second, func() {
		errc <- errors.New("TLS handshake timeout")
	})

	go func() {
		err := tlsConn.Handshake()
		timer.Stop()
		errc <- err
	}()
	if err := <-errc; err != nil {
		plainConn.Close()
		return nil, err
	}

	if !cfg.InsecureSkipVerify {
		if err := tlsConn.VerifyHostname(cfg.ServerName); err != nil {
			plainConn.Close()
			return nil, err
		}
	}

	return wrapConnection(tlsConn, URL), nil // return a wrapped net.Conn
}

func writeWARCFromConnection(req, resp *io.PipeReader, URL *url.URL) (err error) {
	defer WaitGroup.Done()

	var (
		batch      = NewRecordBatch()
		recordChan = make(chan *Record)
	)

	swg := sizedwaitgroup.New(2)

	go func() {
		defer swg.Done()

		// initialize the request record
		var requestRecord = NewRecord()
		requestRecord.Header.Set("WARC-Type", "request")
		requestRecord.Header.Set("WARC-Target-URI", URL.String())
		requestRecord.Header.Set("Host", URL.Host)
		requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, req)
		if err != nil {
			panic(err)
		}

		requestRecord.Content = &buf

		recordChan <- requestRecord
	}()

	go func() {
		defer swg.Done()

		// initialize the response record
		var responseRecord = NewRecord()
		responseRecord.Header.Set("WARC-Type", "response")
		responseRecord.Header.Set("WARC-Target-URI", URL.String())
		responseRecord.Header.Set("Host", URL.Host)
		responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, resp)
		if err != nil {
			panic(err)
		}

		responseRecord.Content = &buf

		recordChan <- responseRecord
	}()

	swg.Wait()

	for i := 0; i < 2; i++ {
		record := <-recordChan
		batch.Records = append(batch.Records, record)
	}

	WARCWriter <- batch

	return nil
}
