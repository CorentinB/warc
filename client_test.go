package warc

import (
	"io"
	"io/ioutil"
	"net/http"
	"net/http/httptest"
	"path"
	"sync"
	"testing"
)

func TestConcurrentWARCWritingWithHTTPClient(t *testing.T) {
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(rotatorSettings, "")
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	var (
		concurrency = 256
		wg          sync.WaitGroup
		errChan     = make(chan error, concurrency)
	)

	for i := 0; i < concurrency; i++ {
		wg.Add(1)

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

	CloseClients(httpClient)
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

	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	httpClient, err := NewWARCWritingHTTPClient(rotatorSettings, "")
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

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

	CloseClients(httpClient)
}
