package warc

import (
	"io"
	"time"
)

func (c *CustomHTTPClient) WriteRecord(WARCTargetURI, WARCType, contentType, payloadString string, payloadReader io.Reader) {
	// Initialize the record
	metadataRecord := NewRecord("", false)

	// Set the headers
	metadataRecord.Header.Set("WARC-Type", WARCType)
	metadataRecord.Header.Set("WARC-Target-URI", WARCTargetURI)

	if contentType != "" {
		metadataRecord.Header.Set("Content-Type", contentType)
	}

	// Write the payload
	if payloadString != "" {
		metadataRecord.Content.Write([]byte(payloadString))
	} else {
		_, err := io.Copy(metadataRecord.Content, payloadReader)
		if err != nil {
			panic(err)
		}
	}

	// Add it to the batch
	doneChan := make(chan bool)
	c.WARCWriter <- &RecordBatch{
		Records:     []*Record{metadataRecord},
		CaptureTime: time.Now().UTC().Format(time.RFC3339Nano),
		Done:        doneChan,
	}

	// Wait for the record to be written
	<-doneChan
}
