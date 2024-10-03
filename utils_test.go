package warc

import (
	"bytes"
	"path/filepath"
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

func checkTempDir(t *testing.T, tempDir string) {
	temp_files, err := filepath.Glob(tempDir + "/*")
	if err != nil {
		t.Fatal(err)
	}

	// If any temp files exist, they are unexpected and should be considered a error.
	if len(temp_files) != 0 {
		t.Fatal("Expected no files in temp directory, but got: ", temp_files)
	}
}
