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
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
)

type customDialer struct {
	net.Dialer
	client *customHTTPClient
}

type customConnection struct {
	net.Conn
	io.Reader
	io.Writer
	closers []io.Closer
	sync.WaitGroup
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

func (d *customDialer) wrapConnection(c net.Conn, scheme string) net.Conn {
	reqReader, reqWriter := io.Pipe()
	respReader, respWriter := io.Pipe()

	d.client.WaitGroup.Add(1)
	go d.writeWARCFromConnection(reqReader, respReader, scheme, c)

	return &customConnection{
		Conn:    c,
		closers: []io.Closer{reqWriter, respWriter},
		Reader:  io.TeeReader(c, respWriter),
		Writer:  io.MultiWriter(reqWriter, c),
	}
}

func (d *customDialer) CustomDial(network, address string) (net.Conn, error) {
	// force IPV4 as we are having some issues figuring out how to get
	// RFC4291 compliant IPV6 IP for WARC-IP-Address
	// NOTE: cause issues for IPV6 only sites?
	if strings.Contains(network, "tcp") {
		network = "tcp4"
	}

	conn, err := d.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return d.wrapConnection(conn, "http"), nil
}

func (d *customDialer) CustomDialTLS(network, address string) (net.Conn, error) {
	// force IPV4 as we are having some issues figuring out how to get
	// RFC4291 compliant IPV6 IP for WARC-IP-Address
	// NOTE: cause issues for IPV6 only sites?
	if strings.Contains(network, "tcp") {
		network = "tcp4"
	}

	plainConn, err := d.Dial(network, address)
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

	return d.wrapConnection(tlsConn, "https"), nil
}

func (d *customDialer) writeWARCFromConnection(reqPipe, respPipe *io.PipeReader, scheme string, conn net.Conn) (err error) {
	defer d.client.WaitGroup.Done()

	var (
		batch         = NewRecordBatch()
		recordChan    = make(chan *Record)
		warcTargetURI = scheme + "://"
		recordIDs     []string
		target        string
		host          string
	)

	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()

		// initialize the request record
		var requestRecord = NewRecord()
		requestRecord.Header.Set("WARC-Type", "request")
		requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, reqPipe)
		if err != nil {
			panic(err)
		}

		// parse data for WARC-Target-URI
		scanner := bufio.NewScanner(bytes.NewReader(buf.Bytes()))
		for scanner.Scan() {
			t := scanner.Text()
			if strings.HasPrefix(t, "GET ") && (strings.HasSuffix(t, "HTTP/1.0") || strings.HasSuffix(t, "HTTP/1.1")) {
				target = strings.Split(t, " ")[1]

				if host != "" && target != "" {
					break
				} else {
					continue
				}
			}

			if strings.HasPrefix(t, "Host: ") {
				host = strings.TrimPrefix(t, "Host: ")

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

	wg.Add(1)
	go func() {
		defer wg.Done()

		// initialize the response record
		var responseRecord = NewRecord()
		responseRecord.Header.Set("WARC-Type", "response")
		responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, respPipe)
		if err != nil {
			panic(err)
		}

		// generate WARC-Payload-Digest
		resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(buf.Bytes())), nil)
		if err != nil {
			panic(err)
		}
		defer resp.Body.Close()

		payloadDigest := GetSHA1FromReader(resp.Body)

		responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+payloadDigest)

		responseRecord.Content = &buf

		recordChan <- responseRecord
	}()

	go func() {
		wg.Wait()
		close(recordChan)
	}()

	for record := range recordChan {
		recordIDs = append(recordIDs, uuid.NewV4().String())
		batch.Records = append(batch.Records, record)
	}

	// add headers
	for i, r := range batch.Records {
		// generate WARC-IP-Address
		switch addr := conn.RemoteAddr().(type) {
		case *net.UDPAddr:
			IP := addr.IP.String()
			r.Header.Set("WARC-IP-Address", IP)
		case *net.TCPAddr:
			IP := addr.IP.String()
			r.Header.Set("WARC-IP-Address", IP)
		}

		// set WARC-Record-ID and WARC-Concurrent-To
		r.Header.Set("WARC-Record-ID", "<urn:uuid:"+recordIDs[i]+">")

		if i == len(recordIDs)-1 {
			r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[0]+">")
		} else {
			r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[1]+">")
		}

		// add WARC-Target-URI
		r.Header.Set("WARC-Target-URI", warcTargetURI)
	}

	d.client.WARCWriter <- batch

	return nil
}

func newCustomDialer(httpClient *customHTTPClient) *customDialer {
	var d = new(customDialer)

	d.Timeout = 30 * time.Second
	d.client = httpClient

	return d
}
