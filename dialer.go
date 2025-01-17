package warc

import (
	"bufio"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"slices"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CorentinB/warc/pkg/spooledtempfile"
	"github.com/google/uuid"
	"github.com/miekg/dns"
	tls "github.com/refraction-networking/utls"
	"golang.org/x/net/proxy"
	"golang.org/x/sync/errgroup"
)

type customDialer struct {
	proxyDialer proxy.Dialer
	client      *CustomHTTPClient
	DNSConfig   *dns.ClientConfig
	DNSClient   *dns.Client
	DNSRecords  *sync.Map
	net.Dialer
	DNSServer     string
	DNSRecordsTTL time.Duration
	disableIPv4   bool
	disableIPv6   bool
}

func newCustomDialer(httpClient *CustomHTTPClient, proxyURL string, DialTimeout, DNSRecordsTTL, DNSResolutionTimeout time.Duration, DNSServers []string, disableIPv4, disableIPv6 bool) (d *customDialer, err error) {
	d = new(customDialer)

	d.Timeout = DialTimeout
	d.client = httpClient
	d.disableIPv4 = disableIPv4
	d.disableIPv6 = disableIPv6

	d.DNSRecordsTTL = DNSRecordsTTL
	d.DNSRecords = new(sync.Map)
	d.DNSConfig, err = dns.ClientConfigFromFile("/etc/resolv.conf")
	if err != nil {
		return nil, err
	}

	if len(DNSServers) > 0 {
		d.DNSConfig.Servers = DNSServers
	}

	d.DNSClient = &dns.Client{
		Net:     "udp",
		Timeout: DNSResolutionTimeout,
	}

	if proxyURL != "" {
		u, err := url.Parse(proxyURL)
		if err != nil {
			return nil, err
		}

		if d.proxyDialer, err = proxy.FromURL(u, d); err != nil {
			return nil, err
		}
	}

	return d, nil
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

func (d *customDialer) CustomDial(network, address string) (conn net.Conn, err error) {
	// Determine the network based on IPv4/IPv6 settings
	network = d.getNetworkType(network)
	if network == "" {
		return nil, errors.New("no supported network type available")
	}

	IP, err := d.archiveDNS(address)
	if err != nil {
		return nil, err
	}

	if d.proxyDialer != nil {
		conn, err = d.proxyDialer.Dial(network, address)
	} else {
		if d.client.randomLocalIP {
			localAddr := getLocalAddr(network, IP.String())
			if localAddr != nil {
				if network == "tcp" || network == "tcp4" || network == "tcp6" {
					d.LocalAddr = localAddr.(*net.TCPAddr)
				} else if network == "udp" || network == "udp4" || network == "udp6" {
					d.LocalAddr = localAddr.(*net.UDPAddr)
				}
			}
		}

		conn, err = d.DialContext(context.Background(), network, address)
	}

	if err != nil {
		return nil, err
	}

	return d.wrapConnection(conn, "http"), nil
}

func (d *customDialer) CustomDialTLS(network, address string) (net.Conn, error) {
	// Determine the network based on IPv4/IPv6 settings
	network = d.getNetworkType(network)
	if network == "" {
		return nil, errors.New("no supported network type available")
	}

	IP, err := d.archiveDNS(address)
	if err != nil {
		return nil, err
	}

	var plainConn net.Conn

	if d.proxyDialer != nil {
		plainConn, err = d.proxyDialer.Dial(network, address)
	} else {
		if d.client.randomLocalIP {
			localAddr := getLocalAddr(network, IP.String())
			if localAddr != nil {
				if network == "tcp" || network == "tcp4" || network == "tcp6" {
					d.LocalAddr = localAddr.(*net.TCPAddr)
				} else if network == "udp" || network == "udp4" || network == "udp6" {
					d.LocalAddr = localAddr.(*net.UDPAddr)
				}
			}
		}

		plainConn, err = d.Dial(network, address)
	}

	if err != nil {
		return nil, err
	}

	cfg := new(tls.Config)
	serverName := address[:strings.LastIndex(address, ":")]
	cfg.ServerName = serverName
	cfg.InsecureSkipVerify = d.client.verifyCerts

	tlsConn := tls.UClient(plainConn, cfg, tls.HelloCustom)

	if err := tlsConn.ApplyPreset(getCustomTLSSpec()); err != nil {
		return nil, err
	}

	errc := make(chan error, 2)
	timer := time.AfterFunc(d.client.TLSHandshakeTimeout, func() {
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

func (d *customDialer) getNetworkType(network string) string {
	switch network {
	case "tcp", "udp":
		if d.disableIPv4 && !d.disableIPv6 {
			return network + "6"
		}
		if !d.disableIPv4 && d.disableIPv6 {
			return network + "4"
		}
		return network // Both enabled or both disabled, use default
	case "tcp4", "udp4":
		if d.disableIPv4 {
			return ""
		}
		return network
	case "tcp6", "udp6":
		if d.disableIPv6 {
			return ""
		}
		return network
	default:
		return "" // Unsupported network type
	}
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
		err                  = new(Error)
	)

	errs.Go(func() error {
		return d.readRequest(scheme, reqPipe, target, host, warcTargetURIChannel, recordChan, err)
	})

	errs.Go(func() error {
		return d.readResponse(respPipe, warcTargetURIChannel, recordChan, err)
	})

	readErr := errs.Wait()
	close(recordChan)
	if readErr != nil {
		// Add the error to the err structure
		err.Err = readErr

		d.client.ErrChan <- err

		// Make sure we close the WARC content buffers
		for record := range recordChan {
			// CHeck if there's an error when closing the content and send to channel if so.
			err := record.Content.Close()
			if err != nil {
				d.client.ErrChan <- &Error{Err: err}
			}
		}

		return
	}

	for record := range recordChan {
		recordIDs = append(recordIDs, uuid.NewString())
		batch.Records = append(batch.Records, record)
	}

	if len(batch.Records) != 2 {
		err.Err = errors.New("warc: there was an unspecified problem creating one of the WARC records")

		d.client.ErrChan <- err

		// Make sure we close the WARC content buffers
		for _, record := range batch.Records {
			err := record.Content.Close()
			if err != nil {
				d.client.ErrChan <- &Error{Err: err}
			}
		}

		return
	}

	// Most Internet Archive tools expect requests to be AFTER responses
	// in the WARC file. So we make sure that's the case.
	if batch.Records[0].Header.Get("WARC-Type") != "response" {
		slices.Reverse(batch.Records)
	}

	// Get the WARC-Target-URI value
	var warcTargetURI = <-warcTargetURIChannel

	// Add headers
	for i, r := range batch.Records {
		// Generate WARC-IP-Address if we aren't using a proxy. If we are using a proxy, the real host IP cannot be determined.
		if d.proxyDialer == nil {
			switch addr := conn.RemoteAddr().(type) {
			case *net.UDPAddr:
				IP := addr.IP.String()
				r.Header.Set("WARC-IP-Address", IP)
			case *net.TCPAddr:
				IP := addr.IP.String()
				r.Header.Set("WARC-IP-Address", IP)
			}
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
			if r.Header.Get("WARC-Type") == "response" && r.Header.Get("WARC-Payload-Digest")[5:] != "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ" {
				d.client.dedupeHashTable.Store(r.Header.Get("WARC-Payload-Digest")[5:], revisitRecord{
					responseUUID: recordIDs[i],
					size:         getContentLength(r.Content),
					targetURI:    warcTargetURI,
					date:         batch.CaptureTime,
				})
			}
		}
	}

	d.client.WARCWriter <- batch
}

func (d *customDialer) readResponse(respPipe *io.PipeReader, warcTargetURIChannel chan string, recordChan chan *Record, upstreamErr *Error) error {
	// Initialize the response record
	var responseRecord = NewRecord(d.client.TempDir, d.client.FullOnDisk)
	responseRecord.Header.Set("WARC-Type", "response")
	responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

	// Read the response from the pipe
	bytesCopied, err := io.Copy(responseRecord.Content, respPipe)
	if err != nil {
		responseRecord.Content.Close()
		return err
	}

	resp, err := http.ReadResponse(bufio.NewReader(responseRecord.Content), nil)
	if err != nil {
		responseRecord.Content.Close()
		return err
	}

	// Grab the WARC-Target-URI and send it back for records post-processing
	var warcTargetURI = <-warcTargetURIChannel
	warcTargetURIChannel <- warcTargetURI

	// If the HTTP status code is to be excluded as per client's settings, we stop here
	for i := 0; i < len(d.client.skipHTTPStatusCodes); i++ {
		if d.client.skipHTTPStatusCodes[i] == resp.StatusCode {
			return fmt.Errorf("response code was blocked by config url: '%s'", warcTargetURI)
		}
	}

	// Calculate the WARC-Payload-Digest
	payloadDigest := GetSHA1(resp.Body)
	if strings.HasPrefix(payloadDigest, "ERROR: ") {
		responseRecord.Content.Close()
		// This should _never_ happen.
		return fmt.Errorf("SHA1 ran into an unrecoverable error: %s url: %s", payloadDigest, warcTargetURI)
	}
	err = resp.Body.Close()
	if err != nil {
		return err
	}
	responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+payloadDigest)

	// Write revisit record if local or CDX dedupe is activated
	var revisit = revisitRecord{}
	if bytesCopied >= int64(d.client.dedupeOptions.SizeThreshold) {
		if d.client.dedupeOptions.LocalDedupe {
			revisit = d.checkLocalRevisit(payloadDigest)

			LocalDedupeTotal.Incr(int64(revisit.size))
		}

		// Allow both to be checked. If local dedupe does not find anything, check CDX (if set).
		if d.client.dedupeOptions.CDXDedupe && revisit.targetURI == "" {
			revisit, _ = checkCDXRevisit(d.client.dedupeOptions.CDXURL, payloadDigest, warcTargetURI, d.client.dedupeOptions.CDXCookie)
			RemoteDedupeTotal.Incr(int64(revisit.size))
		}
	}

	if revisit.targetURI != "" && payloadDigest != "3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ" {
		responseRecord.Header.Set("WARC-Type", "revisit")
		responseRecord.Header.Set("WARC-Refers-To-Target-URI", revisit.targetURI)
		responseRecord.Header.Set("WARC-Refers-To-Date", revisit.date)

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
		tempBuffer := spooledtempfile.NewSpooledTempFile("warc", d.client.TempDir, -1, d.client.FullOnDisk, d.client.MaxRAMUsageFraction)
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
		err = responseRecord.Content.Close()
		if err != nil {
			return err
		}
		responseRecord.Content = tempBuffer
	}

	recordChan <- responseRecord

	return nil
}

func (d *customDialer) readRequest(scheme string, reqPipe *io.PipeReader, target string, host string, warcTargetURIChannel chan string, recordChan chan *Record, upstreamErr *Error) error {
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
				if isLineStartingWithHTTPMethod(line) && (strings.HasSuffix(line, "HTTP/1.0\r") || strings.HasSuffix(line, "HTTP/1.1\r")) {
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
