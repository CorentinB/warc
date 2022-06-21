package warc

import (
	"bufio"
	"bytes"
	"context"
	"crypto/tls"
	"errors"
	"io"
	"net"
	"net/http"
	"strings"
	"sync"
	"time"

	uuid "github.com/satori/go.uuid"
	"golang.org/x/sync/errgroup"
)

type customDialer struct {
	net.Dialer
	client *CustomHTTPClient
}

func newCustomDialer(httpClient *CustomHTTPClient) *customDialer {
	var d = new(customDialer)

	d.Timeout = 5 * time.Second
	d.client = httpClient

	return d
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
	conn, err := d.Dial(network, address)
	if err != nil {
		return nil, err
	}

	return d.wrapConnection(conn, "http"), nil
}

func (d *customDialer) CustomDialTLS(network, address string) (net.Conn, error) {
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

func (d *customDialer) writeWARCFromConnection(reqPipe, respPipe *io.PipeReader, scheme string, conn net.Conn) {
	defer d.client.WaitGroup.Done()

	var (
		batch                = NewRecordBatch()
		recordChan           = make(chan *Record, 2)
		warcTargetURIChannel = make(chan string, 1)
		recordIDs            []string
		target               string
		host                 string
		errs, _              = errgroup.WithContext(context.Background())
	)

	errs.Go(func() error {
		return d.readRequest(scheme, reqPipe, target, host, warcTargetURIChannel, recordChan)
	})

	errs.Go(func() error {
		return d.readResponse(respPipe, warcTargetURIChannel, recordChan)
	})

	err := errs.Wait()
	close(recordChan)

	if err != nil {
		d.client.errChan <- err
		return
	}

	for record := range recordChan {
		recordIDs = append(recordIDs, uuid.NewV4().String())
		batch.Records = append(batch.Records, record)
	}

	if len(batch.Records) != 2 {
		d.client.errChan <- errors.New("warc: there was an unspecified problem creating one of the WARC records")
		return
	}

	// Get the WARC-Target-URI value
	var warcTargetURI = <-warcTargetURIChannel

	// Add headers
	for i, r := range batch.Records {
		// Generate WARC-IP-Address
		switch addr := conn.RemoteAddr().(type) {
		case *net.UDPAddr:
			IP := addr.IP.String()
			r.Header.Set("WARC-IP-Address", IP)
		case *net.TCPAddr:
			IP := addr.IP.String()
			r.Header.Set("WARC-IP-Address", IP)
		}

		// Set WARC-Record-ID and WARC-Concurrent-To
		r.Header.Set("WARC-Record-ID", "<urn:uuid:"+recordIDs[i]+">")

		if i == len(recordIDs)-1 {
			r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[0]+">")
		} else {
			r.Header.Set("WARC-Concurrent-To", "<urn:uuid:"+recordIDs[1]+">")
		}

		// Add WARC-Target-URI
		r.Header.Set("WARC-Target-URI", warcTargetURI)

		if d.client.dedupeOptions.LocalDedupe {
			if r.Header.Get("WARC-Type") == "response" {
				d.client.dedupeHashTable.Store(r.Header.Get("WARC-Payload-Digest")[5:], revisitRecord{
					responseUUID: recordIDs[i],
					targetURI:    warcTargetURI,
					date:         time.Now().UTC(),
				})
			}
		}
	}

	d.client.WARCWriter <- batch

	return
}

func (d *customDialer) readResponse(respPipe *io.PipeReader, warcTargetURIChannel chan string, recordChan chan *Record) error {
	// Initialize the response record
	var responseRecord = NewRecord()
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	// Read the response from the pipe
	_, err := io.Copy(responseRecord.Content, respPipe)
	if err != nil {
		return err
	}

	// The ReadResponse is needed to remove the possible Transfer-Encoding before calculating the WARC-Payload-Digest
	resp, err := http.ReadResponse(bufio.NewReader(bytes.NewReader(responseRecord.Content.Bytes())), nil)
	if err != nil {
		return err
	}

	// If the HTTP status code is to be excluded as per client's settings, we stop here
	for i := 0; i < len(d.client.skipHTTPStatusCodes); i++ {
		if d.client.skipHTTPStatusCodes[i] == resp.StatusCode {
			return errors.New("warc: response code was blocked by config")
		}
	}

	// Calculate the WARC-Payload-Digest
	payloadDigest := GetSHA1FromReader(resp.Body)
	resp.Body.Close()
	responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+payloadDigest)

	// Grab the WARC-Target-URI and send it back for records post-processing
	var warcTargetURI = <-warcTargetURIChannel
	warcTargetURIChannel <- warcTargetURI

	// Write revisit record if local or CDX dedupe is activated
	var revisit = revisitRecord{}
	if d.client.dedupeOptions.LocalDedupe {
		revisit = d.checkLocalRevisit(payloadDigest)
	} else if d.client.dedupeOptions.CDXDedupe {
		revisit, _ = checkCDXRevisit(d.client.dedupeOptions.CDXURL, payloadDigest, warcTargetURI)
	}

	if revisit.targetURI != "" {
		responseRecord.Header.Set("WARC-Type", "revisit")
		responseRecord.Header.Set("WARC-Refers-To-Target-URI", revisit.targetURI)
		responseRecord.Header.Set("WARC-Refers-To-Date", revisit.date.UTC().Format(time.RFC3339))

		if revisit.responseUUID != "" {
			responseRecord.Header.Set("WARC-Refers-To", "<urn:uuid:"+revisit.responseUUID+">")
		}

		responseRecord.Header.Set("WARC-Profile", "http://netpreserve.org/warc/1.1/revisit/identical-payload-digest")
		responseRecord.Header.Set("WARC-Truncated", "length")

		endOfHeadersOffset := bytes.Index(responseRecord.Content.Bytes(), []byte("\r\n\r\n"))

		// This should really never happen! This could be the result of a malfunctioning HTTP server or something currently unknown!
		if endOfHeadersOffset == -1 {
			return errors.New("warc: CRLF not found on response content")
		}

		headers := bytes.NewBuffer(responseRecord.Content.Bytes()[:endOfHeadersOffset])
		responseRecord.Content = headers
	}

	recordChan <- responseRecord

	return nil
}

func (d *customDialer) readRequest(scheme string, reqPipe *io.PipeReader, target string, host string, warcTargetURIChannel chan string, recordChan chan *Record) error {
	var (
		warcTargetURI = scheme + "://"
		requestRecord = NewRecord()
	)

	// Initialize the request record
	requestRecord.Header.Set("WARC-Type", "request")
	requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

	// Copy the content from the pipe
	_, err := io.Copy(requestRecord.Content, reqPipe)
	if err != nil {
		return err
	}

	// Parse data for WARC-Target-URI
	scanner := bufio.NewScanner(bytes.NewReader(requestRecord.Content.Bytes()))
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
		return err
	}

	// Check that we achieved to parse all the necessary data
	if host != "" && target != "" {
		// HTTP's request first line can include a complete path, we check that
		if strings.HasPrefix(target, scheme+"://"+host) {
			warcTargetURI = target
		} else {
			warcTargetURI += host
			warcTargetURI += target
		}
	} else {
		return errors.New("unable to parse data necessary for WARC-Target-URI")
	}

	// Send the WARC-Target-URI to a channel so that it can be picked-up
	// by the goroutine responsible for writing the response
	warcTargetURIChannel <- warcTargetURI

	recordChan <- requestRecord

	return nil
}
