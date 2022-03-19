package warc

import (
	"bufio"
	"bytes"
	"crypto/tls"
	"errors"
	"io"
	"log"
	"net"
	"net/http"
	"strings"
	"time"
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

func wrapConnection(c net.Conn, scheme string) net.Conn {
	reqReader, reqWriter := io.Pipe()
	respReader, respWriter := io.Pipe()

	WaitGroup.Add(1)
	go writeWARCFromConnection(reqReader, respReader, scheme)

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
		return dialer.CustomDialWithURL("tcp", req.Host+":80", req.URL.Scheme)
	case "https":
		return dialer.CustomDialTLSWithURL("tcp", req.Host+":443", req.URL.Scheme)
	default:
		panic("WTF?!?")
	}
}

func (dialer *customDialer) CustomDial(network, address string) (net.Conn, error) {
	return dialer.CustomDialWithURL(network, address, "http")
}

func (dialer *customDialer) CustomDialTLS(network, address string) (net.Conn, error) {
	return dialer.CustomDialTLSWithURL(network, address, "https")
}

func (dialer *customDialer) CustomDialWithURL(network, address string, scheme string) (net.Conn, error) {
	conn, err := dialer.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return wrapConnection(conn, scheme), nil
}

func (dialer *customDialer) CustomDialTLSWithURL(network, address string, scheme string) (net.Conn, error) {
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

	return wrapConnection(tlsConn, scheme), nil // return a wrapped net.Conn
}

func writeWARCFromConnection(req, resp *io.PipeReader, scheme string) (err error) {
	defer WaitGroup.Done()

	var (
		batch         = NewRecordBatch()
		recordChan    = make(chan *Record)
		warcTargetURI = scheme + "://"
		target        string
		host          string
	)

	go func() {
		// initialize the request record
		var requestRecord = NewRecord()
		requestRecord.Header.Set("WARC-Type", "request")
		requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, req)
		if err != nil {
			panic(err)
		}

		// parse data for WARC-Target-URI
		scanner := bufio.NewScanner(&buf)
		for scanner.Scan() {
			if strings.HasPrefix(scanner.Text(), "GET ") && (strings.HasSuffix(scanner.Text(), "HTTP/1.0") || strings.HasSuffix(scanner.Text(), "HTTP/1.1")) {
				splitted := strings.Split(scanner.Text(), " ")
				target = splitted[1]

				if host != "" && target != "" {
					break
				} else {
					continue
				}
			}

			if strings.HasPrefix(scanner.Text(), "Host: ") {
				host = strings.TrimPrefix(scanner.Text(), "Host: ")

				if host != "" && target != "" {
					break
				} else {
					continue
				}
			}
		}

		if err := scanner.Err(); err != nil {
			log.Fatal(err)
		}

		// check that we achieved to parse all the necessary data
		if host != "" && target != "" {
			// HTTP's request first line can include a complete path, we check that
			if strings.HasSuffix(target, scheme+"://"+host) {
				warcTargetURI = target
			} else {
				warcTargetURI += host
				warcTargetURI += target
			}
		} else {
			panic(errors.New("unable to parse data necessary for WARC-Target-URI"))
		}

		requestRecord.Content = &buf

		recordChan <- requestRecord
	}()

	go func() {
		// initialize the response record
		var responseRecord = NewRecord()
		responseRecord.Header.Set("WARC-Type", "response")
		responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, resp)
		if err != nil {
			panic(err)
		}

		responseRecord.Content = &buf

		recordChan <- responseRecord
	}()

	for i := 0; i < 2; i++ {
		record := <-recordChan
		batch.Records = append(batch.Records, record)
	}

	// add the WARC-Target-URI header
	for _, r := range batch.Records {
		r.Header.Set("WARC-Target-URI", warcTargetURI)
	}

	WARCWriter <- batch

	return nil
}
