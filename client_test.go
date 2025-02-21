package warc

import (
	"context"
	"crypto/rand"
	"crypto/rsa"
	"crypto/tls"
	"crypto/x509"
	"crypto/x509/pkix"
	"io"
	"math/big"
	"net"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/armon/go-socks5"
)

// Utility function used in all tests.
func defaultRotatorSettings(t *testing.T) *RotatorSettings {
	var (
		rotatorSettings = NewRotatorSettings()
		err error
	)

	rotatorSettings.Prefix = "TEST"
	rotatorSettings.OutputDirectory, err = os.MkdirTemp("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	return rotatorSettings
}

func defaultBenchmarkRotatorSettings(t *testing.B) *RotatorSettings {
	var (
		rotatorSettings = NewRotatorSettings()
		err error
	)

	rotatorSettings.Prefix = "TEST"
	rotatorSettings.OutputDirectory, err = os.MkdirTemp("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	return rotatorSettings
}

func TestHTTPClient(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL+"/testdata/image.svg", nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/testdata/image.svg")
	}
}

func TestHTTPClientContextCancellation(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// 1) Set up a test server that sends its response slowly, in chunks
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain")
		w.WriteHeader(http.StatusOK)

		// Write chunks of data with delays in between, simulating a slow response
		for i := 0; i < 10; i++ {
			_, writeErr := w.Write([]byte("CHUNK-DATA-"))
			if writeErr != nil {
				return
			}
			w.(http.Flusher).Flush() // force chunk to send
			time.Sleep(300 * time.Millisecond)
		}
	}))
	defer server.Close()

	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	// Read any WARC-writing errors
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for _ = range httpClient.ErrChan {
			// t.Errorf("Error writing to WARC: %s", e.Err.Error())
		}
	}()

	// 3) Create a request with a cancellable context
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, "GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	// 4) Perform the request
	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatalf("unexpected error on Do: %v", err)
	}

	// We’ll read some data, then cancel the context mid-read
	buf := make([]byte, 32)

	// Read a bit
	n, readErr := resp.Body.Read(buf)
	if readErr != nil {
		t.Fatalf("unexpected error on Read: %v", readErr)
	}

	t.Logf("Read %d bytes before cancel: %q", n, buf[:n])

	// 5) Cancel now. This should cause subsequent reads to fail promptly.
	cancel()

	// Attempt to read the rest
	_, readErr = resp.Body.Read(buf)
	if readErr == nil {
		t.Fatal("expected error after context cancellation, got none")
	}
	t.Logf("Got expected read error after cancel: %v", readErr)

	_ = resp.Body.Close()

	httpClient.Close()
	errWg.Wait()
}

func TestHTTPClientTLSHandshakeTimeout(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
		doneChan        = make(chan bool, 1)
	)

	// 1) Set up a TCP listener
	ln, err := net.Listen("tcp", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("failed to listen on TCP: %v", err)
	}
	defer ln.Close()

	// 2) Prepare a minimal self-signed certificate & TLS config
	tlsConfig := generateTLSConfig()

	// 3) Accept connections but deliberately SLEEP before doing TLS handshake
	go func() {
		for {
			conn, err := ln.Accept()
			if err != nil {
				// Listener closed or error – exit the goroutine
				return
			}
			go func(c net.Conn) {
				// Wrap with TLS but never actually respond in time
				tlsConn := tls.Server(c, tlsConfig)
				// Force a long sleep so the handshake never completes
				time.Sleep(5 * time.Second)
				tlsConn.Close()
				doneChan <- true
			}(conn)
		}
	}()

	serverURL := "https://" + ln.Addr().String()

	// 5) Create the WARC-writing HTTP client
	//    The critical part here is enforcing the handshake timeout.
	//    (Exact field names may differ based on your library.)
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:     rotatorSettings,
		TLSHandshakeTimeout: 1 * time.Second, // <--- The key line
		VerifyCerts:         true,            // or "VerifyCerts: false" depending on your lib
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %v", err)
	}

	// Start reading from ErrChan in a background goroutine
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for e := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", e.Err.Error())
		}
	}()

	// 6) Attempt the GET, which should fail due to TLS handshake delay
	req, err := http.NewRequest("GET", serverURL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err == nil {
		// If no error, handshake timeout didn't work
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatal("Expected TLS handshake timeout error, got none")
	} else {
		t.Logf("Got expected error: %v", err)
	}

	httpClient.Close()
	errWg.Wait()

	<-doneChan // Wait for the server goroutine to exit
}

func TestHTTPClientServerClosingConnection(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		// Send normal HTTP headers
		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)

		// Write only a few bytes of the file, then forcibly close the connection
		partialData := fileBytes[:10]
		if _, err := w.Write(partialData); err != nil {
			t.Fatalf("unable to write partial data: %v", err)
		}

		// Hijack the connection and close it immediately
		hijacker, ok := w.(http.Hijacker)
		if !ok {
			t.Fatal("ResponseWriter does not support hijacking")
		}
		conn, _, err := hijacker.Hijack()
		if err != nil {
			t.Fatalf("failed to hijack connection: %v", err)
		}
		conn.Close()
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for _ = range httpClient.ErrChan {
			// We expect an error here, so we don't need to log it
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		// This would be an error that happened during the handshake/headers
		t.Fatalf("Unexpected error: %v", err)
	}

	// Force the read to detect the unexpected connection close
	_, readErr := io.Copy(io.Discard, resp.Body)
	if readErr == nil {
		t.Fatal("Expected network error when reading body, got none")
	} else if strings.Contains(readErr.Error(), "unexpected EOF") {
		t.Logf("Expected network error: %v", readErr)
	} else {
		t.Fatalf("Unexpected error: %v", readErr)
	}

	_ = resp.Body.Close()

	httpClient.Close()
}

func TestHTTPClientDNSFailure(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// Initialize the WARC-writing HTTP client
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	// Goroutine to read errors from ErrChan
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for e := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", e.Err.Error())
		}
	}()

	// Use a guaranteed-nonresolvable domain
	req, err := http.NewRequest("GET", "http://should-not-resolve.example.invalid", nil)
	if err != nil {
		t.Fatal(err)
	}

	// We expect this to fail with a DNS resolution error
	resp, err := httpClient.Do(req)
	if err == nil {
		if resp != nil {
			resp.Body.Close()
		}
		t.Fatal("Expected DNS resolution error, but got none")
	} else {
		t.Logf("Got expected DNS error: %v", err)
	}

	httpClient.Close()
	errWg.Wait()
}

func TestHTTPClientWithProxy(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init socks5 proxy server
	conf := &socks5.Config{}
	proxyServer, err := socks5.New(conf)
	if err != nil {
		panic(err)
	}

	// Create a channel to signal server stop
	stopChan := make(chan struct{})

	go func() {
		listener, err := net.Listen("tcp", "127.0.0.1:8000")
		if err != nil {
			panic(err)
		}
		defer listener.Close()

		go func() {
			<-stopChan
			listener.Close()
		}()

		if err := proxyServer.Serve(listener); err != nil && !strings.Contains(err.Error(), "use of closed network connection") {
			panic(err)
		}
	}()

	// Defer sending the stop signal
	defer close(stopChan)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		Proxy:           "socks5://127.0.0.1:8000"})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/")
	}
}

func TestHTTPClientConcurrent(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		concurrency     = 256
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
	)

	// init test HTTP endpoint
	fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	// Wait for request wait group first before closing out the errorChannel
	wg.Wait()

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 256, server.URL+"/")
	}
}

func TestHTTPClientMultiWARCWriters(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		concurrency     = 256
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
	)
	rotatorSettings.WARCWriterPoolSize = 8

	// init test HTTP endpoint
	fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		_, _ = w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	// Wait for request wait group first before closing out the errorChannel
	wg.Wait()

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	totalRead := 0
	for _, path := range files {
		totalRead += testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, -1, server.URL+"/")
	}

	if totalRead != concurrency {
		t.Fatalf("unexpected number of records read, read: %d but expected: %d", totalRead, concurrency)
	}
}

func TestHTTPClientLocalDedupe(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DedupeOptions: DedupeOptions{
			LocalDedupe: true,
		},
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		io.Copy(io.Discard, resp.Body)

		time.Sleep(time.Second)
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872", "132"}, 2, server.URL+"/")
		testFileRevisitVailidity(t, path, "", "", false)
	}

	// verify that the local dedupe count is correct
	if LocalDedupeTotal.Value() != 26872 {
		t.Fatalf("local dedupe total mismatch, expected: 26872 got: %d", LocalDedupeTotal.Value())
	}
}

func TestHTTPClientRemoteDedupe(t *testing.T) {
	var (
		dedupePath      = "/web/timemap/cdx"
		dedupeResp      = "org,wikimedia,upload)/wikipedia/commons/5/55/blason_ville_fr_sarlat-la-can%c3%a9da_(dordogne).svg 20220320002518 https://upload.wikimedia.org/wikipedia/commons/5/55/Blason_ville_fr_Sarlat-la-Can%C3%A9da_%28Dordogne%29.svg image/svg+xml 200 UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3 13974"
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)
	// init test HTTP endpoint
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	})

	mux.HandleFunc(dedupePath, func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
		w.WriteHeader(http.StatusOK)
		w.Write([]byte(dedupeResp))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DedupeOptions: DedupeOptions{
			LocalDedupe: false,
			CDXDedupe:   true,
			CDXURL:      server.URL,
		},
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	for i := 0; i < 4; i++ {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		io.Copy(io.Discard, resp.Body)

		time.Sleep(time.Second)
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872", "132"}, 4, server.URL+"/")
		testFileRevisitVailidity(t, path, "20220320002518", "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", false)
	}

	// verify that the remote dedupe count is correct
	if RemoteDedupeTotal.Value() != 55896 {
		t.Fatalf("remote dedupe total mismatch, expected: 55896 got: %d", RemoteDedupeTotal.Value())
	}
}

func TestHTTPClientDedupeEmptyPayload(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// Reset counter to 0?
	LocalDedupeTotal.Reset()

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		// Empty. This is intentional to mirror 3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ.
		fileBytes := []byte("")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DedupeOptions: DedupeOptions{
			LocalDedupe:   true,
			SizeThreshold: 1,
		},
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	for i := 0; i < 2; i++ {
		req, err := http.NewRequest("GET", server.URL, nil)
		if err != nil {
			t.Fatal(err)
		}

		resp, err := httpClient.Do(req)
		if err != nil {
			t.Fatal(err)
		}
		defer resp.Body.Close()

		io.Copy(io.Discard, resp.Body)

		time.Sleep(time.Second)
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:3I42H3S6NNFQ2MSVX7XZKYAYSCX5QBYJ", []string{"94", "94"}, 2, server.URL+"/")
		testFileRevisitVailidity(t, path, "", "", true)
	}

	// verify that the local dedupe count is correct
	if LocalDedupeTotal.Value() != 0 {
		t.Fatalf("local dedupe total mismatch, expected: 0 got: %d", LocalDedupeTotal.Value())
	}
}

func TestHTTPClientDisallow429(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:     rotatorSettings,
		SkipHTTPStatusCodes: []int{429},
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			// validate 429 filtering as well as error reporting by url
			if err.Err.Error() != "readResponse: response code was blocked by config url: '"+server.URL+"/'" {
				t.Errorf("Error writing to WARC: %s", err.Err.Error())
			}
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		// note: we are actually expecting nothing here, as such, 0 for expected total. This may error if 429s aren't being filtered correctly!
		testFileSingleHashCheck(t, path, "n/a", []string{"0"}, 0, server.URL+"/")
	}
}

func TestHTTPClientPayloadLargerThan2MB(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", []string{"3096070"}, 1, server.URL+"/")
		os.Remove(path)
	}
}

func TestConcurrentHTTPClientPayloadLargerThan2MB(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		err             error
		concurrency     = 64
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}

	// Wait for request wait group first before closing out the errorChannel
	wg.Wait()

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	totalRead := 0
	for _, path := range files {
		totalRead = testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", []string{"3096070"}, -1, server.URL+"/") + totalRead
	}

	if totalRead != concurrency {
		t.Fatalf("warc: unexpected number of records read. read: %d expected: %d", totalRead, concurrency)
	}
}

func TestHTTPClientWithSelfSignedCertificate(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test (self-signed) HTTPS endpoint
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/")
		os.Remove(path)
	}
}

func TestWARCWritingWithDisallowedCertificate(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test (self-signed) HTTPS endpoint
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings, VerifyCerts: true})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		// There are multiple different strings for x509 running into an invalid certificate and changes per OS.
		if !strings.Contains(err.Error(), "x509: certificate") {
			t.Fatal(err)
		}
	} else {
		defer resp.Body.Close()
		io.Copy(io.Discard, resp.Body)
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		// note: we are actually expecting nothing here, as such, 0 for expected total. This may error if certificates aren't being verified correctly.
		testFileSingleHashCheck(t, path, "n/a", []string{"0"}, 0, server.URL+"/")
	}
}

func TestHTTPClientFullOnDisk(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings, FullOnDisk: true})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/")
	}
}

func TestHTTPClientWithoutIoCopy(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// This test is intended to not output any WARC files.
	// The intent is to ensure that invalid handling of responses generates a "SHA1 error".

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			// validate 429 filtering as well as error reporting by url
			if strings.Contains(err.Err.Error(), "SHA1 ran into an unrecoverable error url") {
				t.Errorf("Error writing to WARC: %s", err.Err.Error())
			}
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}

	// Close the response body before copying body to io.Discard. This causes SHA1 errors!!!!
	resp.Body.Close()

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		// Check for an empty file. This is fine and expected! If we aren't copying to io.Discard correctly, it should run into an error above.
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{""}, 0, server.URL+"/")
	}
}

func TestHTTPClientWithoutChunkEncoding(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("small text string to ensure it isn't chunked"))
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:3TOI6NZK7GYJSFYGATOMMNM2C5VPT3ZD", []string{"180"}, 1, server.URL+"/")
	}
}

func TestHTTPClientWithZStandard(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)
	rotatorSettings.Compression = "ZSTD"

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/")
	}
}

func TestHTTPClientWithZStandardDictionary(t *testing.T) {
	var (
		rotatorSettings = defaultRotatorSettings(t)
		errWg           sync.WaitGroup
		err             error
	)
	rotatorSettings.Compression = "ZSTD"
	// Use predefined compression dictionary in testdata to compress with.
	rotatorSettings.CompressionDictionary = "testdata/dictionary"

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			t.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	req, err := http.NewRequest("GET", server.URL, nil)
	if err != nil {
		t.Fatal(err)
	}

	resp, err := httpClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26872"}, 1, server.URL+"/")
	}
}

func setupIPv4Server(t *testing.T) (string, func()) {
	listener, err := net.Listen("tcp4", "127.0.0.1:0")
	if err != nil {
		t.Fatalf("Failed to set up IPv4 server: %v", err)
	}

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("IPv4 Server"))
		}),
	}

	go server.Serve(listener)

	return "http://" + listener.Addr().String(), func() {
		server.Shutdown(context.Background())
	}
}

func setupIPv6Server(t *testing.T) (string, func()) {
	listener, err := net.Listen("tcp6", "[::1]:0")
	if err != nil {
		t.Fatalf("Failed to set up IPv6 server: %v", err)
	}

	server := &http.Server{
		Handler: http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			w.Write([]byte("IPv6 Server"))
		}),
	}

	go server.Serve(listener)

	return "http://" + listener.Addr().String(), func() {
		server.Shutdown(context.Background())
	}
}

func TestHTTPClientWithIPv4Disabled(t *testing.T) {
	ipv4URL, closeIPv4 := setupIPv4Server(t)
	defer closeIPv4()

	ipv6URL, closeIPv6 := setupIPv6Server(t)
	defer closeIPv6()

	rotatorSettings := defaultRotatorSettings(t)

	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DisableIPv4:     true,
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	// Try IPv4 - should fail
	_, err = httpClient.Get(ipv4URL)
	if err == nil {
		t.Fatalf("Expected error when connecting to IPv4 server, but got none")
	}

	// Try IPv6 - should succeed
	resp, err := httpClient.Get(ipv6URL)
	if err != nil {
		t.Fatalf("Failed to connect to IPv6 server: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "IPv6 Server" {
		t.Fatalf("Unexpected response from IPv6 server: %s", string(body))
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:RTK62UJNR5UCIPX2J64LMV7J4JJ6EXCJ", []string{"147"}, 1, ipv6URL+"/")
	}
}

func TestHTTPClientWithIPv6Disabled(t *testing.T) {
	ipv4URL, closeIPv4 := setupIPv4Server(t)
	defer closeIPv4()

	ipv6URL, closeIPv6 := setupIPv6Server(t)
	defer closeIPv6()

	rotatorSettings := defaultRotatorSettings(t)

	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DisableIPv6:     true,
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	// Try IPv6 - should fail
	_, err = httpClient.Get(ipv6URL)
	if err == nil {
		t.Fatalf("Expected error when connecting to IPv6 server, but got none")
	}

	// Try IPv4 - should succeed
	resp, err := httpClient.Get(ipv4URL)
	if err != nil {
		t.Fatalf("Failed to connect to IPv4 server: %v", err)
	}
	defer resp.Body.Close()

	body, _ := io.ReadAll(resp.Body)
	if string(body) != "IPv4 Server" {
		t.Fatalf("Unexpected response from IPv4 server: %s", string(body))
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:JZIRQ2YRCQ55F6SSNPTXHKMDSKJV6QFM", []string{"147"}, 1, ipv4URL+"/")
	}
}

// MARK: Benchmarks
func BenchmarkConcurrentUnder2MB(b *testing.B) {
	var (
		rotatorSettings = defaultBenchmarkRotatorSettings(b)
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			b.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			b.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	wg.Wait()
	httpClient.Close()
}

func BenchmarkConcurrentUnder2MBZStandard(b *testing.B) {
	var (
		rotatorSettings = defaultBenchmarkRotatorSettings(b)
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		err             error
	)
	rotatorSettings.Compression = "ZSTD"

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			b.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/svg+xml")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			b.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	wg.Wait()
	httpClient.Close()
}

func BenchmarkConcurrentOver2MB(b *testing.B) {
	var (
		rotatorSettings = defaultBenchmarkRotatorSettings(b)
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			b.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			b.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	wg.Wait()
	httpClient.Close()
}

func BenchmarkConcurrentOver2MBZStandard(b *testing.B) {
	var (
		rotatorSettings = defaultBenchmarkRotatorSettings(b)
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		err             error
	)
	rotatorSettings.Compression = "ZSTD"

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := os.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			b.Fatal(err)
		}

		w.Header().Set("Content-Type", "image/jpeg")
		w.WriteHeader(http.StatusOK)
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range httpClient.ErrChan {
			b.Errorf("Error writing to WARC: %s", err.Err.Error())
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				httpClient.ErrChan <- &Error{Err: err}
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	wg.Wait()
	httpClient.Close()
}

// generateTLSConfig creates a self-signed certificate for testing.
func generateTLSConfig() *tls.Config {
	// 1) Generate a private key.
	privKey, err := rsa.GenerateKey(rand.Reader, 2048)
	if err != nil {
		panic(err)
	}

	// 2) Create a certificate template.
	now := time.Now()
	tmpl := &x509.Certificate{
		SerialNumber: big.NewInt(1),
		Subject: pkix.Name{
			CommonName:   "Test Self-Signed Cert",
			Organization: []string{"Local Testing"},
		},
		NotBefore: now,
		NotAfter:  now.Add(time.Hour), // valid for 1 hour

		KeyUsage:    x509.KeyUsageKeyEncipherment | x509.KeyUsageDigitalSignature,
		ExtKeyUsage: []x509.ExtKeyUsage{x509.ExtKeyUsageServerAuth},

		IsCA:                  true, // so we can sign ourselves
		BasicConstraintsValid: true,
	}

	// 3) Self-sign the certificate using our private key.
	derBytes, err := x509.CreateCertificate(rand.Reader, tmpl, tmpl, &privKey.PublicKey, privKey)
	if err != nil {
		panic(err)
	}

	// 4) Parse the DER-encoded certificate.
	cert, err := x509.ParseCertificate(derBytes)
	if err != nil {
		panic(err)
	}

	// 5) Create a tls.Certificate that our server can use.
	keyPair := tls.Certificate{
		Certificate: [][]byte{derBytes},
		PrivateKey:  privKey,
		Leaf:        cert,
	}

	// Return a TLS config that uses our self-signed cert.
	return &tls.Config{
		Certificates: []tls.Certificate{keyPair},

		// NOTE: If you want the server to present this certificate for any
		// named host (SNI), you might also need to set other fields or
		// an option like InsecureSkipVerify on the client side if you don't
		// plan to trust this certificate chain.
	}
}
