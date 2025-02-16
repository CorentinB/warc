package warc

import (
	"bytes"
	"testing"
)

// Tests for the GetSHA1 function
func TestGetSHA1(t *testing.T) {
	helloWorldSHA1 := "FKXGYNOJJ7H3IFO35FPUBC445EPOQRXN"

	if GetSHA1(bytes.NewReader([]byte("hello world"))) != helloWorldSHA1 {
		t.Error("Failed to generate SHA1 with GetSHA1")
	}
}

// Tests for the GetSHA256 function
func TestGetSHA256(t *testing.T) {
	helloWorldSHA256 := "XFGSPOMTJU7ARJJOKLL5U7NL7LCIJ37DPJJYB3UQRD32ZYXPZXUQ===="

	if GetSHA256(bytes.NewReader([]byte("hello world"))) != helloWorldSHA256 {
		t.Error("Failed to generate SHA256 with GetSHA256")
	}
}

func TestGetSHA256Base16(t *testing.T) {
	helloWorldSHA256Base16 := "b94d27b9934d3e08a52e52d7da7dabfac484efe37a5380ee9088f7ace2efcde9"

	if GetSHA256Base16(bytes.NewReader([]byte("hello world"))) != helloWorldSHA256Base16 {
		t.Error("Failed to generate SHA256Base16 with GetSHA256Base16")
	}
}

// Tests for the NewRotatorSettings function
func TestNewRotatorSettings(t *testing.T) {
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
