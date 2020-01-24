package warc_test

import (
	"bytes"
	"crypto/sha1"
	"fmt"
	"github.com/slyrz/warc"
	"io"
	"io/ioutil"
	"os"
	"strings"
	"testing"
)

func testFileHash(t *testing.T, path string, mode warc.Mode) {
	t.Logf("testFileHash %q, mode %v", path, mode)

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := warc.NewReaderMode(file, mode)
	if err != nil {
		t.Fatalf("warc.NewReaderMode failed for %q: %v", path, err)
	}
	defer reader.Close()

	for {
		record, err := reader.ReadRecord()
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
		hash := fmt.Sprintf("sha1:%x", sha1.Sum(content))
		if hash != record.Header["warc-block-digest"] {
			t.Fatalf("expected %q, got %q", record.Header["warc-block-digest"], hash)
		}
	}
}

func testFileScan(t *testing.T, path string, mode warc.Mode) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	reader, err := warc.NewReaderMode(file, mode)
	if err != nil {
		t.Fatalf("warc.NewReaderMode failed for %q: %v", path, err)
	}
	defer reader.Close()

	total := 0
	for {
		if _, err := reader.ReadRecord(); err != nil {
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
		"testdata/test.warc",
		"testdata/test.warc.gz",
		"testdata/test.warc.bz2",
	}
	for _, path := range paths {
		testFileHash(t, path, warc.SequentialMode)
		testFileHash(t, path, warc.AsynchronousMode)
		testFileScan(t, path, warc.SequentialMode)
		testFileScan(t, path, warc.AsynchronousMode)
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

func TestWriteRead(t *testing.T) {
	buffer := new(bytes.Buffer)

	writer := warc.NewWriter(buffer)
	for i, testRecord := range testRecords {
		t.Logf("writing record %d", i)
		record := warc.NewRecord()
		record.Header = testRecord.Header
		record.Content = bytes.NewReader(testRecord.Content)
		if _, err := writer.WriteRecord(record); err != nil {
			t.Fatal(err)
		}
	}

	reader, err := warc.NewReader(buffer)
	if reader.Compression() != warc.CompressionNone {
		t.Errorf("reader.Compression() == %q, expected %q",
			reader.Compression(), warc.CompressionNone)
	}
	if err != nil {
		t.Fatalf("failed to create reader: %v", err)
	}
	for i, testRecord := range testRecords {
		t.Logf("reading record %d", i)
		record, err := reader.ReadRecord()
		if err != nil {
			t.Fatalf("expected record, got %v", err)
		}
		for key, val := range testRecord.Header {
			if record.Header[key] != val {
				t.Errorf("expected %q = %q, got %q", key, val, record.Header[key])
			}
		}
		content, err := ioutil.ReadAll(record.Content)
		if err != nil {
			t.Fatal(err)
		}
		if string(content) != string(testRecord.Content) {
			t.Errorf("expected %s = %s", content, testRecord.Content)
		}
	}
}

func ExampleReader() {
	reader, err := warc.NewReader(os.Stdin)
	if err != nil {
		panic(err)
	}
	defer reader.Close()

	for {
		record, err := reader.ReadRecord()
		if err != nil {
			break
		}
		fmt.Println("Record:")
		for key, value := range record.Header {
			fmt.Printf("%v = %v\n", key, value)
		}
	}
}

func ExampleWriter() {
	writer := warc.NewWriter(os.Stdout)
	record := warc.NewRecord()
	record.Header.Set("warc-type", "resource")
	record.Header.Set("content-type", "plain/text")
	record.Content = strings.NewReader("Hello, World!")
	if _, err := writer.WriteRecord(record); err != nil {
		panic(err)
	}
}
