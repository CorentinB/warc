package warc

import (
	"bufio"
	"bytes"
	"compress/gzip"
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/djherbis/buffer"
	"github.com/klauspost/compress/zstd"
)

func GetSHA1FromReader(r io.Reader) string {
	sha := sha1.New()

	io.Copy(sha, r)

	return base32.StdEncoding.EncodeToString(sha.Sum(nil))
}

// GetSHA1 return the SHA1 of a []byte,
// can be used to fill the WARC-Block-Digest header
func GetSHA1(b buffer.Buffer) string {
	sha := sha1.New()

	block := make([]byte, 256)
	for {
		n, err := b.Read(block)
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

// GetSHA1FromFile return the SHA1 of a file,
// can be used to fill the WARC-Block-Digest header
func GetSHA1FromFile(path string, headers []byte) (string, error) {
	hash := sha1.New()

	file, err := os.Open(path)
	if err != nil {
		return "", err
	}
	defer file.Close()
	if headers != nil {
		hash.Write(headers)
		hash.Write([]byte("\r\n\r\n"))
	}

	if _, err := io.Copy(hash, file); err != nil {
		return "", err
	}

	return base32.StdEncoding.EncodeToString(hash.Sum(nil)), nil
}

// splitKeyValue parses WARC record header fields.
func splitKeyValue(line string) (string, string) {
	parts := strings.SplitN(line, ":", 2)
	if len(parts) != 2 {
		return "", ""
	}
	return parts[0], strings.TrimSpace(parts[1])
}

// NewWriter creates a new WARC writer.
func NewWriter(writer io.Writer, fileName string, compression string) (*Writer, error) {
	if compression != "" {
		if compression == "GZIP" {
			gzipWriter := gzip.NewWriter(writer)
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
func NewRecord() *Record {
	return &Record{
		Header: NewHeader(),
		// Buffer 1MB to Memory, after that buffer to 100MB chunked files
		Content: NewMemorySlurper("blobref"),
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
	if fileSize >= maxSize {
		return true
	}

	return false
}

// formatSerial add the correct padding to the serial
// E.g. with serial = 23 and format = 5:
// formatSerial return 00023
func formatSerial(serial int, format string) string {
	return fmt.Sprintf("%0"+format+"d", serial)
}

// generateWarcFileName generate a WARC file name following recommendations
// of the specs:
// Prefix-Timestamp-Serial-Crawlhost.warc.gz
func generateWarcFileName(prefix string, compression string, serial int) (fileName string) {
	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	formattedSerial := formatSerial(serial, "5")

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

// https://stackoverflow.com/questions/39064343/how-to-get-the-size-of-an-io-reader-object
// not great! looking for something better
func getSize(stream io.Reader) int {
	buf := new(bytes.Buffer)
	buf.ReadFrom(stream)
	return buf.Len()
}
