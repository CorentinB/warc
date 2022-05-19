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
	"os"
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

func (d *customDialer) writeWARCFromConnection(reqPipe, respPipe *io.PipeReader, scheme string, conn net.Conn) (err error) {
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
		var warcTargetURI = scheme + "://"

		// initialize the request record
		var requestRecord = NewRecord()
		requestRecord.Header.Set("WARC-Type", "request")
		requestRecord.Header.Set("Content-Type", "application/http; msgtype=request")

		var buf bytes.Buffer
		_, err := io.Copy(&buf, reqPipe)
		if err != nil {
			return err
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
			return err
		}

		// check that we achieved to parse all the necessary data
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

		// send the WARC-Target-URI to a channel so that it can be picked-up
		// by the goroutine responsible for writing the response
		warcTargetURIChannel <- warcTargetURI

		requestRecord.Content = &buf

		recordChan <- requestRecord

		return nil
	})

	errs.Go(func() error {
		// initialize the response record
		var responseRecord = NewRecord()
		responseRecord.Header.Set("WARC-Type", "response")
		responseRecord.Header.Set("Content-Type", "application/http; msgtype=response")

		var buf bytes.Buffer
		// read 2MB to memory and if there's more to be read move it to a file
		read, err := io.CopyN(&buf, respPipe, 2000*1024)
		if err != io.EOF && err != nil {
			return err
		}

		if read == 2000*1024 {
			rest_of_file := bytes.NewBuffer(bytes.Split(buf.Bytes(), []byte("\r\n\r\n"))[1])

			//TODO: investigate why files that are incredibly large claim to only take 50ms when we actually have to take much longer

			//Send headers through content so they can be written first as well as read for later steps (status code and proper Payload-Digest)
			buf = *bytes.NewBuffer(bytes.Split(buf.Bytes(), []byte("\r\n\r\n"))[0])
			responseRecord.Content = &buf

			tmpFile, err := os.CreateTemp(d.client.WARCTempDir, "warc-temp-*")
			if err != nil {
				return err
			}

			_, err = io.Copy(tmpFile, rest_of_file)
			if err != nil {
				os.Remove(tmpFile.Name())
				return err
			}

			_, err = io.Copy(tmpFile, respPipe)
			if err != nil {
				os.Remove(tmpFile.Name())
				return err
			}

			responseRecord.PayloadPath = tmpFile.Name()
			tmpFile.Close()
		} else {
			responseRecord.Content = &buf
		}

		// Define resp here so that we can define it for both methods of writing to WARC
		var resp *http.Response

		// generate WARC-Payload-Digest and check status code
		// here we are checking how we are putting the data into the WARC and reading the HTTP response correctly, either from file, or reading the buffer.
		if responseRecord.PayloadPath != "" {
			tmpBytes, err := os.ReadFile(responseRecord.PayloadPath)
			if err != nil {
				return err
			}

			resp, err = http.ReadResponse(bufio.NewReader(bytes.NewReader(append(append(buf.Bytes(), []byte("\r\n\r\n")...), tmpBytes...))), nil)
			if err != nil {
				return err
			}
		} else {
			resp, err = http.ReadResponse(bufio.NewReader(bytes.NewReader(buf.Bytes())), nil)
			if err != nil {
				return err
			}
		}

		for i := 0; i < len(d.client.skipHTTPStatusCodes); i++ {
			if d.client.skipHTTPStatusCodes[i] == resp.StatusCode {
				return errors.New("warc: response code was blocked by config")
			}
		}

		payloadDigest := GetSHA1FromReader(resp.Body)

		responseRecord.Header.Set("WARC-Payload-Digest", "sha1:"+payloadDigest)
		resp.Body.Close()

		var revisit = revisitRecord{}
		if d.client.dedupeOptions.LocalDedupe {
			revisit = d.checkLocalRevisit(payloadDigest)
		}

		// grab the WARC-Target-URI and send it back for records post-processing
		var warcTargetURI = <-warcTargetURIChannel
		warcTargetURIChannel <- warcTargetURI

		if revisit.targetURI == "" {
			if d.client.dedupeOptions.CDXDedupe {
				revisit, err = checkCDXRevisit(d.client.dedupeOptions.CDXURL, payloadDigest, warcTargetURI)
				if err != nil {
					// possibly ignore in the future?
					return err
				}
			}
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

			//just headers
			headers := bytes.NewBuffer(bytes.Split(buf.Bytes(), []byte("\r\n\r\n"))[0])

			if responseRecord.PayloadPath != "" {
				os.Remove(responseRecord.PayloadPath)
				// This isn't required as Content is checked first, but we are going to delete it, so it seems like the correct thing to do.
				responseRecord.PayloadPath = ""
			}

			responseRecord.Content = headers
		}

		recordChan <- responseRecord

		return nil
	})

	err = errs.Wait()
	close(recordChan)

	if err != nil {
		// note: at the moment these errors don't go anywhere because wrapConnection calls us as a goroutine
		return err
	}

	for record := range recordChan {
		recordIDs = append(recordIDs, uuid.NewV4().String())
		batch.Records = append(batch.Records, record)
	}

	if len(batch.Records) != 2 {
		return errors.New("warc: there was a problem creating one of the WARC records")
	}

	// get the WARC-Target-URI value
	var warcTargetURI = <-warcTargetURIChannel

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

	return nil
}

func newCustomDialer(httpClient *CustomHTTPClient) *customDialer {
	var d = new(customDialer)

	d.Timeout = 5 * time.Second
	d.client = httpClient

	return d
}
