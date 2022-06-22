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
	"sync"
	"testing"
	"time"
)

func TestConcurrentWARCWritingWithHTTPClient(t *testing.T) {
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
	rotatorSettings := NewRotatorSettings()
	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "CONC"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
			t.Errorf("Error writing to WARC: %s", err)
		}
	}()

	var (
		concurrency = 256
		wg          sync.WaitGroup
		errChan     = make(chan error, concurrency)
	)

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

	files, err := filepath.Glob("warcs/CONC-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", 256)
	}
}

func TestWARCWritingWithHTTPClient(t *testing.T) {
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
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

	files, err := filepath.Glob("warcs/TEST-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", 1)
	}
}

func TestWARCWritingWithHTTPClientLocalDedupe(t *testing.T) {
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "DEDUP1"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{LocalDedupe: true, CDXDedupe: false}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
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

	files, err := filepath.Glob("warcs/DEDUP1-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", 2)
	}
}

func TestWARCWritingWithHTTPClientRemoteDedupe(t *testing.T) {
	var (
		dedupePath = "/web/timemap/cdx"
		dedupeResp = "org,wikimedia,upload)/wikipedia/commons/5/55/blason_ville_fr_sarlat-la-can%c3%a9da_(dordogne).svg 20220320002518 https://upload.wikimedia.org/wikipedia/commons/5/55/Blason_ville_fr_Sarlat-la-Can%C3%A9da_%28Dordogne%29.svg image/svg+xml 200 UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3 13974"
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "DEDUP2"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{LocalDedupe: true, CDXDedupe: true, CDXURL: server.URL}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
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

	files, err := filepath.Glob("warcs/DEDUP2-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:UIRWL5DFIPQ4MX3D3GFHM2HCVU3TZ6I3", 2)
	}
}

func TestWARCWritingWithHTTPClientDisallow429(t *testing.T) {
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST429"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{429}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
			if err.Error() != "warc: response code was blocked by config" {
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

	files, err := filepath.Glob("warcs/TEST429-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		// note: we are actually expecting nothing here, as such, 0 for expected total. This may error if 429s aren't being filtered correctly!
		testFileSingleHashCheck(t, path, "n/a", 0)
	}
}

func TestWARCWritingWithHTTPClientLargerThan2MB(t *testing.T) {
	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "file_over_2mb.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST2MB"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
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

	files, err := filepath.Glob("warcs/TEST2MB-*")
	if err != nil {
		t.Fatal(err)
	}

	for _, path := range files {
		testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", 1)
		os.Remove(path)
	}
}

func TestConcurrentWARCWritingWithHTTPClientLargerThan2MB(t *testing.T) {
	// init test HTTP endpoint
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		fileBytes, err := ioutil.ReadFile(path.Join("testdata", "file_over_2mb.jpg"))
		if err != nil {
			t.Fatal(err)
		}

		w.WriteHeader(http.StatusOK)
		w.Header().Set("Content-Type", "image/svg+xml")
		w.Write(fileBytes)
	}))
	defer server.Close()

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "CONCTEST2MB"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{}, false)
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var errWg sync.WaitGroup
	errWg.Add(1)
	go func() {
		defer errWg.Done()
		for err := range warcErrChan {
			t.Errorf("Error writing to WARC: %s", err)
		}
	}()

	var (
		concurrency = 64
		wg          sync.WaitGroup
		errChan     = make(chan error, concurrency)
	)

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

	files, err := filepath.Glob("warcs/CONCTEST2MB-*")
	if err != nil {
		t.Fatal(err)
	}

	totalRead := 0
	for _, path := range files {
		totalRead = testFileSingleHashCheck(t, path, "sha1:2WGRFHHSLP26L36FH4ZYQQ5C6WSQAGT7", -1) + totalRead
		os.Remove(path)
	}

	if totalRead != concurrency {
		t.Fatalf("warc: unexpected number of records read. read: " + strconv.Itoa(totalRead) + " expected: " + strconv.Itoa(concurrency))
	}
}
