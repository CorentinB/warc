package warc

// func TestWriteDebugFile(t *testing.T) {
// 	// init WARC rotator settings
// 	rotatorSettings := NewRotatorSettings()
// 	rotatorSettings.OutputDirectory = "warcs"
// 	rotatorSettings.Compression = "GZIP"
// 	rotatorSettings.Prefix = "DEBUG"

// 	// init the HTTP client responsible for recording HTTP(s) requests / responses
// 	httpClient, warcErrChan, err := NewWARCWritingHTTPClient(rotatorSettings, "", false, DedupeOptions{}, []int{}, true)
// 	if err != nil {
// 		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
// 	}
// }
