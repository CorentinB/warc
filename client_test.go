package warc

import (
	"io"
	"net/http"
	"testing"
)

func TestWARCWritingHTTPClient(t *testing.T) {
	// init WARC rotator settings
	var rotatorSettings = NewRotatorSettings()
	var err error

	rotatorSettings.OutputDirectory = "warcs"
	rotatorSettings.Compression = "GZIP"
	rotatorSettings.Prefix = "TEST"

	// init the HTTP client responsible for recording HTTP(s) requests / responses
	err = NewWARCWritingHTTPClient(rotatorSettings, "")
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	req, err := http.NewRequest("GET", "https://img-s-msn-com.akamaized.net/tenant/amp/entityid/AAVeYY8.img?h=1536&w=2560&m=6&q=60&o=f&l=f&x=236&y=113", nil)

	if err != nil {
		t.Fatal(err)
	}

	resp, err := HTTPClient.Do(req)

	if err != nil {
		t.Fatal(err)
	}

	defer resp.Body.Close()

	io.Copy(io.Discard, resp.Body)

	Close()
}
