package warc

import (
	"bytes"
	"fmt"
	"io"
	"os"
	"strings"
	"testing"

	"go.uber.org/goleak"
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
		record, eol, err := reader.ReadRecord()
		if eol {
			break
		}
		if err != nil {
			t.Fatalf("failed to read all record content: %v", err)
			break
		}

		hash := fmt.Sprintf("sha1:%s", GetSHA1(record.Content))
		if hash != record.Header["WARC-Block-Digest"] {
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			t.Fatalf("expected %s, got %s", record.Header.Get("WARC-Block-Digest"), hash)
		}
		err = record.Content.Close()
		if err != nil {
			t.Fatalf("failed to close record content: %v", err)
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
		_, eol, err := reader.ReadRecord()
		if eol {
			break
		}
		if err != nil {
			t.Fatalf("failed to read all record content: %v", err)
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
		record, eol, err := reader.ReadRecord()
		if eol {
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
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			t.Fatalf("warc.ReadRecord failed: %v", err)
			break
		}

		if record.Header.Get("WARC-Type") != "response" && record.Header.Get("WARC-Type") != "revisit" {
			// We're not currently interesting in anything but response and revisit records at the moment.
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			continue
		}

		if record.Header.Get("WARC-Payload-Digest") != hash {
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
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
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			t.Fatalf("Content-Length doesn't match intended result %s != %s", record.Header.Get("Content-Length"), expectedContentLength)
		}

		err = record.Content.Close()
		if err != nil {
			t.Fatalf("failed to close record content: %v", err)
		}
		totalRead++
	}
	return -1
}

func testFileRevisitVailidity(t *testing.T, path string, originalTime string, originalDigest string, shouldBeEmpty bool) {
	var revisitRecordsFound = false
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	defer file.Close()

	t.Logf("checking 'WARC-Refers-To-Date' and 'WARC-Payload-Digest' for revisits on %q", path)

	reader, err := NewReader(file)
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}

	for {
		record, eol, err := reader.ReadRecord()
		if eol {
			if revisitRecordsFound {
				return
			}
			if shouldBeEmpty {
				t.Logf("No revisit records found. That's expected for this test.")
				break
			}

			t.Fatalf("No revisit records found.")
			break
		}

		if err != nil {
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			t.Fatalf("warc.ReadRecord failed: %v", err)
			break
		}

		if record.Header.Get("WARC-Type") != "response" && record.Header.Get("WARC-Type") != "revisit" {
			// We're not currently interesting in anything but response and revisit records at the moment.
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			continue
		}

		if record.Header.Get("WARC-Type") == "response" {
			originalDigest = record.Header.Get("WARC-Payload-Digest")
			originalTime = record.Header.Get("WARC-Date")
			err = record.Content.Close()
			if err != nil {
				t.Fatalf("failed to close record content: %v", err)
			}
			continue
		}

		if record.Header.Get("WARC-Type") == "revisit" {
			revisitRecordsFound = true
			if record.Header.Get("WARC-Payload-Digest") == originalDigest && record.Header.Get("WARC-Refers-To-Date") == originalTime {
				err = record.Content.Close()
				if err != nil {
					t.Fatalf("failed to close record content: %v", err)
				}
				continue
			} else {
				err = record.Content.Close()
				if err != nil {
					t.Fatalf("failed to close record content: %v", err)
				}
				t.Fatalf("Revisit digest or date does not match doesn't match intended result %s != %s (or %s != %s)", record.Header.Get("WARC-Payload-Digest"), originalDigest, record.Header.Get("WARC-Refers-To-Date"), originalTime)
			}
		}

	}
}

func testFileEarlyEOF(t *testing.T, path string) {
	file, err := os.Open(path)
	if err != nil {
		t.Fatalf("failed to open %q: %v", path, err)
	}
	reader, err := NewReader(file)
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}
	// read the file into memory
	data, err := io.ReadAll(reader.bufReader)
	if err != nil {
		t.Fatalf("failed to read %q: %v", path, err)
	}
	// delete the last two bytes (\r\n)
	if data[len(data)-2] != '\r' || data[len(data)-1] != '\n' {
		t.Fatalf("expected \\r\\n, got %q", data[len(data)-2:])
	}
	data = data[:len(data)-2]
	// new reader
	reader, err = NewReader(io.NopCloser(bytes.NewReader(data)))
	if err != nil {
		t.Fatalf("warc.NewReader failed for %q: %v", path, err)
	}
	// read the records
	for {
		_, eol, err := reader.ReadRecord()
		if eol {
			break
		}
		if err != nil {
			if strings.Contains(err.Error(), "early EOF record boundary") {
				return // ok
			} else {
				t.Fatalf("expected early EOF record boundary, got %v", err)
			}
		}
	}
	t.Fatalf("expected early EOF record boundary, got none")
}

func TestReader(t *testing.T) {
	defer goleak.VerifyNone(t)

	var paths = []string{
		"testdata/test.warc.gz",
	}
	for _, path := range paths {
		testFileHash(t, path)
		testFileScan(t, path)
		testFileEarlyEOF(t, path)
	}
}

func BenchmarkBasicRead(b *testing.B) {
	// default test warc location
	path := "testdata/test.warc.gz"

	for n := 0; n < b.N; n++ {
		b.Logf("checking 'WARC-Block-Digest' on %q", path)

		file, err := os.Open(path)
		if err != nil {
			b.Fatalf("failed to open %q: %v", path, err)
		}
		defer file.Close()

		reader, err := NewReader(file)
		if err != nil {
			b.Fatalf("warc.NewReader failed for %q: %v", path, err)
		}

		for {
			record, eol, err := reader.ReadRecord()
			if eol {
				break
			}
			if err != nil {
				b.Fatalf("failed to read all record content: %v", err)
				break
			}

			hash := fmt.Sprintf("sha1:%s", GetSHA1(record.Content))
			if hash != record.Header["WARC-Block-Digest"] {
				err = record.Content.Close()
				if err != nil {
					b.Fatalf("failed to close record content: %v", err)
				}
				b.Fatalf("expected %s, got %s", record.Header.Get("WARC-Block-Digest"), hash)
			}
			err = record.Content.Close()
			if err != nil {
				b.Fatalf("failed to close record content: %v", err)
			}
		}
	}
}
