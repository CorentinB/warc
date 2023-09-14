package warc

import (
	"bufio"
	"bytes"
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"sync/atomic"
	"time"

	gzip "github.com/klauspost/compress/gzip"
	"github.com/klauspost/pgzip"

	"github.com/klauspost/compress/zstd"
)

func GetSHA1(r io.Reader) string {
	sha := sha1.New()

	block := make([]byte, 256)
	for {
		n, err := r.Read(block)
		if n > 0 {
			sha.Write(block[:n])
		}

		if err == io.EOF {
			break
		}

		if err != nil {
			return "ERROR"
		}
	}

	return base32.StdEncoding.EncodeToString(sha.Sum(nil))
}

// splitKeyValue parses WARC record header fields.
func splitKeyValue(line string) (string, string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}

func isLineStartingWithHTTPMethod(line string) bool {
	if strings.HasPrefix(line, "GET ") ||
		strings.HasPrefix(line, "HEAD ") ||
		strings.HasPrefix(line, "POST ") ||
		strings.HasPrefix(line, "PUT ") ||
		strings.HasPrefix(line, "DELETE ") ||
		strings.HasPrefix(line, "CONNECT ") ||
		strings.HasPrefix(line, "OPTIONS ") ||
		strings.HasPrefix(line, "TRACE ") ||
		strings.HasPrefix(line, "PATCH ") {
		return true
	}

	return false
}

// NewWriter creates a new WARC writer.
func NewWriter(writer io.Writer, fileName string, compression string, contentLengthHeader string) (*Writer, error) {
	if compression != "" {
		if compression == "GZIP" {
			var gzipWriter *gzip.Writer
			// If the record's Content-Length is bigger than a megabyte, we use the parallel gzip library
			if contentLengthHeader != "" {
				contentLength, err := strconv.Atoi(contentLengthHeader)
				if err != nil {
					return nil, err
				}

				if contentLength > 1000000 {
					pgzipWriter := pgzip.NewWriter(writer)
					return &Writer{
						FileName:    fileName,
						Compression: compression,
						PGZIPWriter: pgzipWriter,
						FileWriter:  bufio.NewWriter(pgzipWriter),
					}, nil
				}
			}

			gzipWriter = gzip.NewWriter(writer)
			return &Writer{
				FileName:    fileName,
				Compression: compression,
				GZIPWriter:  gzipWriter,
				FileWriter:  bufio.NewWriter(gzipWriter),
			}, nil
		} else if compression == "ZSTD" {
			zstdWriter, err := zstd.NewWriter(writer)
			if err != nil {
				return nil, err
			}
			return &Writer{
				FileName:    fileName,
				Compression: compression,
				ZSTDWriter:  zstdWriter,
				FileWriter:  bufio.NewWriter(zstdWriter),
			}, nil
		}
		return nil, errors.New("Invalid compression algorithm: " + compression)
	}

	return &Writer{
		FileName:    fileName,
		Compression: "",
		FileWriter:  bufio.NewWriter(writer),
	}, nil
}

// NewRecord creates a new WARC record.
func NewRecord(tempDir string, fullOnDisk bool) *Record {
	return &Record{
		Header:  NewHeader(),
		Content: NewSpooledTempFile("warc", tempDir, fullOnDisk),
	}
}

// NewRecordBatch creates a record batch,
// it also initialize the capture time
func NewRecordBatch() *RecordBatch {
	return &RecordBatch{
		CaptureTime: time.Now().UTC().Format(time.RFC3339Nano),
	}
}

// NewRotatorSettings creates a RotatorSettings structure
// and initialize it with default values
func NewRotatorSettings() *RotatorSettings {
	return &RotatorSettings{
		WarcinfoContent: NewHeader(),
		Prefix:          "WARC",
		WarcSize:        1000,
		Compression:     "GZIP",
		OutputDirectory: "./",
	}
}

// checkRotatorSettings validate RotatorSettings settings, and set
// default values if needed
func checkRotatorSettings(settings *RotatorSettings) (err error) {
	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Check if output directory is specified, if not, set it to the current directory
	if settings.OutputDirectory == "" {
		settings.OutputDirectory = "./"
	} else {
		// If it is specified, check if output directory exist
		if _, err := os.Stat(settings.OutputDirectory); os.IsNotExist(err) {
			// If it doesn't exist, create it
			// MkdirAll will create all parent directories if needed
			err = os.MkdirAll(settings.OutputDirectory, os.ModePerm)
			if err != nil {
				return err
			}
		}
	}

	if settings.WARCWriterPoolSize == 0 {
		settings.WARCWriterPoolSize = 1
	}

	// Add a trailing slash to the output directory
	if settings.OutputDirectory[len(settings.OutputDirectory)-1:] != "/" {
		settings.OutputDirectory = settings.OutputDirectory + "/"
	}

	// If prefix isn't specified, set it to "WARC"
	if settings.Prefix == "" {
		settings.Prefix = "WARC"
	}

	// If WARC size isn't specified, set it to 1GB (10^9 bytes) by default
	if settings.WarcSize == 0 {
		settings.WarcSize = 1000
	}

	// Check if the specified compression algorithm is valid
	if settings.Compression != "" && settings.Compression != "GZIP" && settings.Compression != "ZSTD" {
		return errors.New("Invalid compression algorithm: " + settings.Compression)
	}

	// Add few headers to the warcinfo payload, to not have it empty
	settings.WarcinfoContent.Set("hostname", hostName)
	settings.WarcinfoContent.Set("format", "WARC file version 1.1")
	settings.WarcinfoContent.Set("conformsTo", "http://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.1/")

	return nil
}

// isFielSizeExceeded compare the size of a file (filePath) with
// a max size (maxSize), if the size of filePath exceed maxSize,
// it returns true, else, it returns false
func isFileSizeExceeded(filePath string, maxSize float64) bool {
	// Open the file
	file, err := os.Open(filePath)
	if err != nil {
		panic(err)
	}
	defer file.Close()

	// Get actual file size
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := (float64)((stat.Size() / 1024) / 1024)

	// If fileSize exceed maxSize, return true
	return fileSize >= maxSize
}

// formatSerial add the correct padding to the serial
// E.g. with serial = 23 and format = 5:
// formatSerial return 00023
func formatSerial(atomicSerial *int64, format string) string {
	return fmt.Sprintf("%0"+format+"d", atomic.LoadInt64(atomicSerial))
}

// GenerateWarcFileName generate a WARC file name following recommendations
// of the specs:
// Prefix-Timestamp-Serial-Crawlhost.warc.gz
func GenerateWarcFileName(prefix string, compression string, atomicSerial *int64) (fileName string) {
	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Don't let atomicSerial overflow past 99999, the current maximum with 5 serial digits.
	if atomic.LoadInt64(atomicSerial) >= 99999 {
		atomic.StoreInt64(atomicSerial, 0)
	}

	// Atomically increase the global serial number
	atomic.AddInt64(atomicSerial, 1)

	formattedSerial := formatSerial(atomicSerial, "5")

	now := time.Now().UTC()
	date := now.Format("20060102150405") + strconv.Itoa(now.Nanosecond())[:3]

	if compression != "" {
		if compression == "GZIP" {
			return prefix + "-" + date + "-" + formattedSerial + "-" + hostName + ".warc.gz.open"
		}
		if compression == "ZSTD" {
			return prefix + "-" + date + "-" + formattedSerial + "-" + hostName + ".warc.zst.open"
		}
	}
	return prefix + "-" + date + "-" + formattedSerial + "-" + hostName + ".warc.open"
}

func getContentLength(rwsc ReadWriteSeekCloser) int {
	// If the FileName leads to no existing file, it means that the SpooledTempFile
	// never had the chance to buffer to disk instead of memory, in which case we can
	// just read the buffer (which should be <= 2MB) and return the length
	if rwsc.FileName() == "" {
		rwsc.Seek(0, 0)
		buf := new(bytes.Buffer)
		buf.ReadFrom(rwsc)
		return buf.Len()
	} else {
		// Else, we return the size of the file on disk
		fileInfo, err := os.Stat(rwsc.FileName())
		if err != nil {
			panic(err)
		}

		return int(fileInfo.Size())
	}
}
