package warc

import (
	"fmt"
	"io"
	"os"
	"testing"
)

func testFileHash(t *testing.T, path string) {
	t.Logf("checking 'WARC-Block-Digest' on %q", path)

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
		record, err := reader.ReadRecord()
		if err != nil {
			if err != io.EOF {
				t.Fatalf("failed to read all record content: %v", err)
			}
			break
		}

		hash := fmt.Sprintf("sha1:%s", GetSHA1(record.Content))
		if hash != record.Header["WARC-Block-Digest"] {
			record.Content.Close()
			t.Fatalf("expected %s, got %s", record.Header.Get("WARC-Block-Digest"), hash)
		}
		record.Content.Close()
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
		if _, err := reader.ReadRecord(); err != nil {
			break
		}
		total++
	}

	if total != 3 {
		t.Fatalf("expected 3 records, got %v", total)
	}
}

func testFileSingleHashCheck(t *testing.T, path string, hash string, expectedContentLength []string, expectedTotal int) int {
	// The below function validates the Block-Digest per record while the function we are in checks for a specific Payload-Digest in records :)
	testFileHash(t, path)

	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	t.Logf("checking 'WARC-Payload-Digest' on %q", path)

	reader, err := NewReader(file)
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}

	totalRead := 0

	for {
		record, err := reader.ReadRecord()
		if err == io.EOF {
			if expectedTotal == -1 {
				// This is expected for multiple file WARCs as we need to count the total count outside of this function.
				return totalRead
			}

			if totalRead == expectedTotal {
				// We've read the expected amount and reached the end of the WARC file. Time to break out.
				break
			} else {
				t.Fatalf("unexpected number of records read, read: %d but expected: %d", totalRead, expectedTotal)
				return -1
			}
		}

		if err != nil {
			record.Content.Close()
			t.Fatalf("warc.ReadRecord failed: %v", err)
			break
		}

		if record.Header.Get("WARC-Type") != "response" && record.Header.Get("WARC-Type") != "revisit" {
			// We're not currently interesting in anything but response and revisit records at the moment.
			record.Content.Close()
			continue
		}

		if record.Header.Get("WARC-Payload-Digest") != hash {
			record.Content.Close()
			t.Fatalf("WARC-Payload-Digest doesn't match intended result %s != %s", record.Header.Get("WARC-Payload-Digest"), hash)
		}

		badContentLength := false
		for i := 0; i < len(expectedContentLength); i++ {
			if record.Header.Get("Content-Length") != expectedContentLength[i] {
				badContentLength = true
			} else {
				badContentLength = false
				break
			}
		}

		if badContentLength {
			record.Content.Close()
			t.Fatalf("Content-Length doesn't match intended result %s != %s", record.Header.Get("Content-Length"), expectedContentLength)
		}

		record.Content.Close()
		totalRead++
	}
	return -1
}

func TestReader(t *testing.T) {
	var paths = []string{
		"testdata/test.warc.gz",
	}
	for _, path := range paths {
		testFileHash(t, path)
		testFileScan(t, path)
	}
}
