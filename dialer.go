package warc

import (
	"bufio"
	"context"
	"errors"
	"io"
	"net"
	"net/http"
	"strconv"
	"strings"
	"sync"
	"time"

	tls "github.com/refraction-networking/utls"

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
	cfg.InsecureSkipVerify = d.client.verifyCerts

	tlsConn := tls.UClient(plainConn, cfg, tls.HelloCustom)

	//Taken from https://github.com/refraction-networking/utls/blob/master/u_parrots.go#L215 and modified to fit our needs.
	customTLSSpec := tls.ClientHelloSpec{
		CipherSuites: []uint16{
			tls.GREASE_PLACEHOLDER,
			tls.TLS_AES_128_GCM_SHA256,
			tls.TLS_AES_256_GCM_SHA384,
			tls.TLS_CHACHA20_POLY1305_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_ECDHE_ECDSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_ECDHE_ECDSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_CHACHA20_POLY1305,
			tls.TLS_ECDHE_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_ECDHE_RSA_WITH_AES_256_CBC_SHA,
			tls.TLS_RSA_WITH_AES_128_GCM_SHA256,
			tls.TLS_RSA_WITH_AES_256_GCM_SHA384,
			tls.TLS_RSA_WITH_AES_128_CBC_SHA,
			tls.TLS_RSA_WITH_AES_256_CBC_SHA,
		},
		CompressionMethods: []byte{
			0x00, // compressionNone
		},
		Extensions: []tls.TLSExtension{
			&tls.UtlsGREASEExtension{},
			&tls.SNIExtension{},
			&tls.UtlsExtendedMasterSecretExtension{},
			&tls.RenegotiationInfoExtension{Renegotiation: tls.RenegotiateOnceAsClient},
			&tls.SupportedCurvesExtension{Curves: []tls.CurveID{
				tls.CurveID(tls.GREASE_PLACEHOLDER),
				tls.X25519,
				tls.CurveP256,
				tls.CurveP384,
			}},
			&tls.SupportedPointsExtension{SupportedPoints: []byte{
				0x00, // pointFormatUncompressed
			}},
			&tls.SessionTicketExtension{},
			//changed IMPORTANT!!! default ALPN is "h2", "http/1.1". This could get servers to automatically send us HTTP2, which we can't parse or handle. We could be profiled based on this as the signature technically changes.
			&tls.ALPNExtension{AlpnProtocols: []string{"http/1.1"}},
			&tls.StatusRequestExtension{},
			&tls.SignatureAlgorithmsExtension{SupportedSignatureAlgorithms: []tls.SignatureScheme{
				tls.ECDSAWithP256AndSHA256,
				tls.PSSWithSHA256,
				tls.PKCS1WithSHA256,
				tls.ECDSAWithP384AndSHA384,
				tls.PSSWithSHA384,
				tls.PKCS1WithSHA384,
				tls.PSSWithSHA512,
				tls.PKCS1WithSHA512,
			}},
			&tls.SCTExtension{},
			&tls.KeyShareExtension{KeyShares: []tls.KeyShare{
				{Group: tls.CurveID(tls.GREASE_PLACEHOLDER), Data: []byte{0}},
				{Group: tls.X25519},
			}},
			&tls.PSKKeyExchangeModesExtension{Modes: []uint8{
				tls.PskModeDHE,
			}},
			&tls.SupportedVersionsExtension{Versions: []uint16{
				tls.GREASE_PLACEHOLDER,
				tls.VersionTLS13,
				tls.VersionTLS12,
				tls.VersionTLS11,
				tls.VersionTLS10,
			}},
			&tls.FakeCertCompressionAlgsExtension{Methods: []tls.CertCompressionAlgo{
				tls.CertCompressionBrotli,
			}},
			&tls.UtlsGREASEExtension{},
			&tls.UtlsPaddingExtension{GetPaddingLen: tls.BoringPaddingStyle},
		},
	}

	if err := tlsConn.ApplyPreset(&customTLSSpec); err != nil {
		return nil, err
	}
	if err := tlsConn.Handshake(); err != nil {
		return nil, err
	}

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
		// Make sure we close the WARC content buffers
		for record := range recordChan {
			record.Content.Close()
		}

		return
	}

	for record := range recordChan {
		recordIDs = append(recordIDs, uuid.NewV4().String())
		batch.Records = append(batch.Records, record)
	}

	if len(batch.Records) != 2 {
		d.client.errChan <- errors.New("warc: there was an unspecified problem creating one of the WARC records")

		// Make sure we close the WARC content buffers
		for _, record := range batch.Records {
			record.Content.Close()
		}

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

		// Calculate WARC-Block-Digest and Content-Length
		// Those 2 steps are done at this stage of the process ON PURPOSE, to take
		// advantage of the parallelization context in which this function is called.
		// That way, we reduce I/O bottleneck later when the record is at the "writing" step,
		// because the actual WARC writing sequential, not parallel.
		r.Content.Seek(0, 0)
		r.Header.Set("WARC-Block-Digest", "sha1:"+GetSHA1(r.Content))
		r.Header.Set("Content-Length", strconv.Itoa(getContentLength(r.Content)))

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
}

func (d *customDialer) readResponse(respPipe *io.PipeReader, warcTargetURIChannel chan string, recordChan chan *Record) error {
	// Initialize the response record
	var responseRecord = NewRecord(d.client.TempDir, d.client.FullOnDisk)
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	// Read the response from the pipe
	_, err := io.Copy(responseRecord.Content, respPipe)
	if err != nil {
		return err
	}

	resp, err := http.ReadResponse(bufio.NewReader(responseRecord.Content), nil)
	if err != nil {
		return err
	}

	// If the HTTP status code is to be excluded as per client's settings, we stop here
	for i := 0; i < len(d.client.skipHTTPStatusCodes); i++ {
		if d.client.skipHTTPStatusCodes[i] == resp.StatusCode {
			return errors.New("response code was blocked by config")
		}
	}

	// Calculate the WARC-Payload-Digest
	payloadDigest := GetSHA1(resp.Body)
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

		// Find the position of the end of the headers
		responseRecord.Content.Seek(0, 0)
		found := false
		bigBlock := make([]byte, 0, 4)
		block := make([]byte, 1)
		endOfHeadersOffset := 0
		for {
			n, err := responseRecord.Content.Read(block)
			if n > 0 {
				switch len(bigBlock) {
				case 0:
					if string(block) == "\r" {
						bigBlock = append(bigBlock, block...)
					}
				case 1:
					if string(block) == "\n" {
						bigBlock = append(bigBlock, block...)
					} else {
						bigBlock = nil
					}
				case 2:
					if string(block) == "\r" {
						bigBlock = append(bigBlock, block...)
					} else {
						bigBlock = nil
					}
				case 3:
					if string(block) == "\n" {
						bigBlock = append(bigBlock, block...)
						found = true
					} else {
						bigBlock = nil
					}
				}

				endOfHeadersOffset++

				if found {
					break
				}
			}

			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}
		}

		// This should really never happen! This could be the result of a malfunctioning HTTP server or something currently unknown!
		if endOfHeadersOffset == -1 {
			return errors.New("CRLF not found on response content")
		}

		// Write the data up until the end of the headers to a temporary buffer
		tempBuffer := NewSpooledTempFile("warc", d.client.TempDir, d.client.FullOnDisk)
		block = make([]byte, 1)
		wrote := 0
		responseRecord.Content.Seek(0, 0)
		for {
			n, err := responseRecord.Content.Read(block)
			if n > 0 {
				_, err = tempBuffer.Write(block)
				if err != nil {
					return err
				}
			}

			if err == io.EOF {
				break
			}

			if err != nil {
				return err
			}

			wrote++

			if wrote == endOfHeadersOffset {
				break
			}
		}

		// Close old buffer
		responseRecord.Content.Close()
		responseRecord.Content = tempBuffer
	}

	recordChan <- responseRecord

	return nil
}

func (d *customDialer) readRequest(scheme string, reqPipe *io.PipeReader, target string, host string, warcTargetURIChannel chan string, recordChan chan *Record) error {
	var (
		warcTargetURI = scheme + "://"
		requestRecord = NewRecord(d.client.TempDir, d.client.FullOnDisk)
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
	var (
		block = make([]byte, 1)
		line  string
	)

	for {
		n, err := requestRecord.Content.Read(block)
		if n > 0 {
			if string(block) == "\n" {
				if strings.HasPrefix(line, "GET ") && (strings.HasSuffix(line, "HTTP/1.0\r") || strings.HasSuffix(line, "HTTP/1.1\r")) {
					target = strings.Split(line, " ")[1]

					if host != "" && target != "" {
						break
					} else {
						line = ""
						continue
					}
				}

				if strings.HasPrefix(line, "Host: ") {
					host = strings.TrimPrefix(line, "Host: ")
					host = strings.TrimSuffix(host, "\r")

					if host != "" && target != "" {
						break
					} else {
						line = ""
						continue
					}
				}

				line = ""
			} else {
				line += string(block)
			}
		} else {
			break
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return err
		}
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
