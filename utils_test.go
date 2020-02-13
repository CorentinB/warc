package warc

import "testing"

// Tests for the GetSHA1 function
func TestGetSHA1(t *testing.T) {
	helloWorldSHA1 := "FKXGYNOJJ7H3IFO35FPUBC445EPOQRXN"

	if GetSHA1([]byte("hello world")) != helloWorldSHA1 {
		t.Error("Failed to generate SHA1 with GetSHA1")
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

	if rotatorSettings.Encryption != false {
		t.Error("Failed to set WARC rotator's encryption usage setting")
	}
}
