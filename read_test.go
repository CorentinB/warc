package warc

import (
	"fmt"
	"io"
	"io/ioutil"
	"os"
	"testing"
)

func testFileHash(t *testing.T, path string) {
	t.Logf("testFileHash on %q", path)

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := NewReader(file)
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}

	for {
		record, err := reader.ReadRecord(false)
		if err != nil {
			if err != io.EOF {
				t.Fatalf("failed to read all record content: %v", err)
			}
			break
		}

		content, err := ioutil.ReadAll(record.Content)
		if err != nil {
			t.Fatalf("failed to read all record content: %v", err)
		}

		hash := fmt.Sprintf("sha1:%s", GetSHA1(content))
		if hash != record.Header["WARC-Block-Digest"] {
			t.Fatalf("expected %s, got %s", record.Header.Get("WARC-Block-Digest"), hash)
		}
	}
}

func testFileScan(t *testing.T, path string) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := NewReader(file)
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}

	total := 0
	for {
		if _, err := reader.ReadRecord(false); err != nil {
			break
		}
		total++
	}
	if total != 10 {
		t.Fatalf("expected 50 records, got %v", total)
	}
}

func TestReader(t *testing.T) {
	var paths = []string{
		"testdata/test.warc.gz",
	}

	for _, path := range paths {
		testFileHash(t, path)
	}
}

var testRecords = []struct {
	Header  map[string]string
	Content []byte
}{
	{
		Header: map[string]string{
			"foo": "bar",
			"baz": "qux",
		},
		Content: []byte("Hello, World!"),
	},
	{
		Header: map[string]string{
			"some-key":    "some value",
			"another-key": "another value",
		},
		Content: []byte("Multiline\nText\n"),
	},
	{
		Header: map[string]string{
			"key 1": "value 1",
			"key 2": "value 2",
			"key 3": "value 3",
			"key 4": "value 4",
			"key 5": "value 5",
		},
		Content: []byte{
			0, 1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15,
		},
	},
}

// func TestSimpleWriteRead(t *testing.T) {
// 	buffer := new(bytes.Buffer)

// 	// We write the test records to the warc writer
// 	writer, err := NewWriter(buffer, "test.warc.gz", "GZIP")
// 	if err != nil {
// 		t.Fatalf("failed to initialize a new writer: %v", err)
// 	}

// 	for i, testRecord := range testRecords {
// 		t.Logf("writing record %d", i)
// 		record := NewRecord()
// 		record.Header = testRecord.Header
// 		record.Content = bytes.NewReader(testRecord.Content)

// 		_, err := writer.WriteRecord(record)
// 		if err != nil {
// 			t.Fatalf("error while writing test record: %v", err)
// 		}

// 		writer.GZIPWriter.Close()
// 		writer, err = NewWriter(buffer, "test.warc.gz", "GZIP")
// 		if err != nil {
// 			t.Fatalf("failed to initialize a new writer: %v", err)
// 		}
// 	}

// 	// Now we try to read the records of the previously written
// 	// warc writer, and test if we get the expected result
// 	reader, err := NewReader(buffer)
// 	if err != nil {
// 		t.Fatalf("failed to create reader: %v", err)
// 	}
// 	defer reader.Close()

// 	// We read the records and test if we get the expected output
// 	for i, testRecord := range testRecords {
// 		t.Logf("reading record %d", i)
// 		record, err := reader.ReadRecord(false)
// 		if err != nil {
// 			t.Fatalf("expected record, got %v", err)
// 		}

// 		// Test the headers
// 		for key, val := range testRecord.Header {
// 			if record.Header[key] != val {
// 				t.Errorf("expected %q = %q, got %q", key, val, record.Header[key])
// 			}
// 		}

// 		// Test the record content
// 		content, err := ioutil.ReadAll(record.Content)
// 		if err != nil {
// 			t.Fatalf("failed reading the test record: %v", err)
// 		}

// 		if string(content) != string(testRecord.Content) {
// 			t.Errorf("expected %s = %s", content, testRecord.Content)
// 		}
// 	}
// }
