package warc

import (
	"bytes"
	"testing"

	"go.uber.org/goleak"
)

// Tests for the GetSHA1 function
func TestGetSHA1(t *testing.T) {
	defer goleak.VerifyNone(t)

	helloWorldSHA1 := "FKXGYNOJJ7H3IFO35FPUBC445EPOQRXN"

	if GetSHA1(bytes.NewReader([]byte("hello world"))) != helloWorldSHA1 {
		t.Error("Failed to generate SHA1 with GetSHA1")
	}
}

// Tests for the NewRotatorSettings function
func TestNewRotatorSettings(t *testing.T) {
	defer goleak.VerifyNone(t)

	rotatorSettings := NewRotatorSettings()

	if rotatorSettings.Prefix != "WARC" {
		t.Error("Failed to set WARC rotator's filename prefix")
	}

	if rotatorSettings.WarcSize != 1000 {
		t.Error("Failed to set WARC rotator's WARC size")
	}

	if rotatorSettings.OutputDirectory != "./" {
		t.Error("Failed to set WARC rotator's output directory")
	}

	if rotatorSettings.Compression != "GZIP" {
		t.Error("Failed to set WARC rotator's compression algorithm")
	}

	if rotatorSettings.CompressionDictionary != "" {
		t.Error("Failed to set WARC rotator's compression dictionary")
	}
}

// Tests for the isLineStartingWithHTTPMethod function
func TestIsHTTPRequest(t *testing.T) {
	defer goleak.VerifyNone(t)

	goodHTTPRequestHeaders := []string{
		"GET /index.html HTTP/1.1\r",
		"POST /api/login HTTP/1.1\r",
		"DELETE /api/products/456 HTTP/1.1\r",
		"HEAD /about HTTP/1.0\r",
		"OPTIONS / HTTP/1.1\r",
		"PATCH /api/item/789 HTTP/1.1\r",
		"GET /images/logo.png HTTP/1.1\r",
	}

	for _, header := range goodHTTPRequestHeaders {
		if !isHTTPRequest(header) {
			t.Error("Invalid HTTP Method parsing:", header)
		}
	}
}
