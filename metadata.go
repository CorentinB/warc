package warc

import (
	"time"
)

func (c *CustomHTTPClient) WriteMetadataRecord(WARCTargetURI, contentType, payload string) {
	// Initialize the record
	metadataRecord := NewRecord("", false)

	// Set the headers
	metadataRecord.Header.Set("WARC-Type", "metadata")
	metadataRecord.Header.Set("Content-Type", contentType)
	metadataRecord.Header.Set("WARC-Target-URI", WARCTargetURI)

	// Write the payload
	metadataRecord.Content.Write([]byte(payload))

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
