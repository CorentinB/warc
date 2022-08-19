package warc

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"os"
	"path"
	"path/filepath"
	"strconv"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/armon/go-socks5"
)

func TestHTTPClient(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errChan, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errChan {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, 1)
	}
}

func TestHTTPClientWithProxy(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init socks5 proxy server
	conf := &socks5.Config{}
	proxyServer, err := socks5.New(conf)
	if err != nil {
		panic(err)
	}

	go func() {
		if err := proxyServer.ListenAndServe("tcp", "127.0.0.1:8000"); err != nil {
			panic(err)
		}
	}()

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "PROXY"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errChan, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		Proxy:           "socks5://127.0.0.1:8000"})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errChan {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, 1)
	}
}

func TestHTTPClientConcurrent(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		concurrency     = 256
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		errChan         = make(chan error, concurrency)
	)

	// init test HTTP endpoint
	fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = w.Write(fileBytes)
	}))
	defer server.Close()

	// init WARC rotator settings
	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "CONC"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				errChan <- err
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, 256)
	}
}

func TestHTTPClientMultiWARCWriters(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		concurrency     = 256
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		errChan         = make(chan error, concurrency)
	)

	// init test HTTP endpoint
	fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
	if err != nil {
		t.Fatal(err)
	}

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		_, _ = w.Write(fileBytes)
	}))
	defer server.Close()

	// init WARC rotator settings
	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "MWW"
	rotatorSettings.WARCWriterPoolSize = 8

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				errChan <- err
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	totalRead := 0
	for _, path := range files {
		totalRead += testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, -1)
	}

	if totalRead != concurrency {
		t.Fatalf("unexpected number of records read, read: %d but expected: %d", totalRead, concurrency)
	}
}

func TestHTTPClientLocalDedupe(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "DEDUP1"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{
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
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882", "142"}, 2)
	}
}

func TestHTTPClientRemoteDedupe(t *testing.T) {
	var (
		dedupePath      = "/web/timemap/cdx"
		dedupeResp      = "org,wikimedia,upload)/wikipedia/commons/5/55/blason_ville_fr_sarlat-la-can%c3%a9da_(dordogne).svg 20220320002518 https://upload.wikimedia.org/wikipedia/commons/5/55/Blason_ville_fr_Sarlat-la-Can%C3%A9da_%28Dordogne%29.svg image/svg+xml 200 UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3 13974"
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)
	// init test HTTP endpoint
	mux := http.NewServeMux()

	mux.HandleFunc("/", func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	})

	mux.HandleFunc(dedupePath, func(w http.ResponseWriter, r *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "text/plain;charset=UTF-8")
		w.Write([]byte(dedupeResp))
	})

	server := httptest.NewServer(mux)
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "DEDUP2"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
		DedupeOptions: DedupeOptions{
			LocalDedupe: true,
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
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882", "142"}, 2)
	}
}

func TestHTTPClientDisallow429(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TEST429"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings:     rotatorSettings,
		SkipHTTPStatusCodes: []int{429},
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			if err.Error() != "response code was blocked by config" {
				t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "n/a", []string{"0"}, 0)
	}
}

func TestHTTPClientPayloadLargerThan2MB(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/jpeg")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TEST2MB"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", []string{"3096070"}, 1)
		os.Remove(path)
	}
}

func TestConcurrentHTTPClientPayloadLargerThan2MB(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		err             error
		concurrency     = 64
		wg              sync.WaitGroup
		errWg           sync.WaitGroup
		errChan         = make(chan error, concurrency)
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/jpeg")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "CONCTEST2MB"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
		}
	}()

	wg.Add(concurrency)
	for i := 0; i < concurrency; i++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			req.Close = true
			if err != nil {
				errChan <- err
				return
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				errChan <- err
				return
			}

			io.Copy(io.Discard, resp.Body)
			resp.Body.Close()
		}()
	}

	go func() {
		wg.Wait()
		close(errChan)
	}()

	for err := range errChan {
		if err != nil {
			t.Fatal(err)
		}
	}

	httpClient.Close()

	files, err := filepath.Glob(rotatorSettings.OutputDirectory + "/*")
	if err != nil {
		t.Fatal(err)
	}

	totalRead := 0
	for _, path := range files {
		totalRead = testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", []string{"3096070"}, -1) + totalRead
	}

	if totalRead != concurrency {
		t.Fatalf("warc: unexpected number of records read. read: " + strconv.Itoa(totalRead) + " expected: " + strconv.Itoa(concurrency))
	}
}

func TestHTTPClientWithSelfSignedCertificate(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test (self-signed) HTTPS endpoint
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TESTCERT1"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, 1)
		os.Remove(path)
	}
}

func TestWARCWritingWithDisallowedCertificate(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		errWg           sync.WaitGroup
		err             error
	)

	// init test (self-signed) HTTPS endpoint
	server := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusTooManyRequests)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TESTCERT2"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errorChannel, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings, VerifyCerts: true})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errorChannel {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "n/a", []string{"0"}, 0)
	}
}

func TestHTTPClientFullOnDisk(t *testing.T) {
	var (
		rotatorSettings = NewRotatorSettings()
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(rotatorSettings.OutputDirectory)

	rotatorSettings.Prefix = "TESTONDISK"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errChan, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings, FullOnDisk: true})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range errChan {
			t.Errorf("Error writing to WARC: %s", err)
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
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", []string{"26882"}, 1)
	}
}

func BenchmarkConcurrentUnder2MB(b *testing.B) {
	var (
		rotatorSettings = NewRotatorSettings()
		wg              sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "image.svg"))
		if err != nil {
			b.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = os.RemoveAll(rotatorSettings.OutputDirectory)
		if err != nil {
			b.Fatal(err)
		}
	}()

	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errChan, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errChan {
			b.Errorf("Error writing to WARC: %s", err)
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				errChan <- err
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				errChan <- err
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
		rotatorSettings = NewRotatorSettings()
		wg              sync.WaitGroup
		err             error
	)

	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "2MB.jpg"))
		if err != nil {
			b.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/jpeg")
		w.Write(fileBytes)
	}))
	defer server.Close()

	rotatorSettings.OutputDirectory, err = ioutil.TempDir("", "warc-tests-")
	if err != nil {
		b.Fatal(err)
	}

	defer func() {
		err = os.RemoveAll(rotatorSettings.OutputDirectory)
		if err != nil {
			b.Fatal(err)
		}
	}()

	rotatorSettings.Prefix = "CONCTEST2MB"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, errChan, err := NewWARCWritingHTTPClient(HTTPClientSettings{RotatorSettings: rotatorSettings})
	if err != nil {
		b.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	wg.Add(1)
	go func() {
		defer wg.Done()
		for err := range errChan {
			b.Errorf("Error writing to WARC: %s", err)
		}
	}()

	wg.Add(b.N)
	for n := 0; n < b.N; n++ {
		go func() {
			defer wg.Done()

			req, err := http.NewRequest("GET", server.URL, nil)
			if err != nil {
				errChan <- err
			}

			resp, err := httpClient.Do(req)
			if err != nil {
				errChan <- err
			}
			defer resp.Body.Close()

			io.Copy(io.Discard, resp.Body)
		}()
	}

	wg.Wait()
	httpClient.Close()
}
