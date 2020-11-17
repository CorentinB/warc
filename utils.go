package warc

import (
	"bufio"
	"compress/gzip"
	"crypto/sha1"
	"encoding/base32"
	"errors"
	"fmt"
	"io"
	"os"
	"strconv"
	"time"

	"github.com/klauspost/compress/zstd"
)

func tempFileDone(filePath string) {
	err := os.Rename(filePath, filePath+".done")
	if err != nil {
		panic(err)
	}
}

// GetSHA1 return the SHA1 of a []byte,
// can be used to fill the WARC-Payload-Digest header
func GetSHA1(content []byte) string {
	sha := sha1.New()
	sha.Write(content)

	return base32.StdEncoding.EncodeToString(sha.Sum(nil))
}

// NewWriter creates a new WARC writer.
func NewWriter(writer io.Writer, fileName string, compression string) (*Writer, error) {
	if compression != "" {
		if compression == "GZIP" {
			gzipWriter := gzip.NewWriter(writer)
			return &Writer{
				FileName:    fileName,
				Compression: compression,
				gzipWriter:  gzipWriter,
				fileWriter:  bufio.NewWriter(gzipWriter),
			}, nil
		} else if compression == "ZSTD" {
			zstdWriter, err := zstd.NewWriter(writer)
			if err != nil {
				return nil, err
			}
			return &Writer{
				FileName:    fileName,
				Compression: compression,
				zstdWriter:  zstdWriter,
				fileWriter:  bufio.NewWriter(zstdWriter),
			}, nil
		}
		return nil, errors.New("Invalid compression algorithm: " + compression)
	}

	return &Writer{
		FileName:    fileName,
		Compression: "",
		fileWriter:  bufio.NewWriter(writer),
	}, nil
}

// NewReader creates a new WARC reader.
func NewReader(reader io.Reader) (*Reader, error) {
	return NewReaderMode(reader, DefaultMode)
}

// NewRecord creates a new WARC record.
func NewRecord() *Record {
	return &Record{
		Header: NewHeader(),
	}
}

// NewRecordBatch creates a record batch,
// it also initialize the capture time
func NewRecordBatch() *RecordBatch {
	return &RecordBatch{
		CaptureTime: time.Now().UTC().Format(time.RFC3339),
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
	settings.WarcinfoContent.Set("format", "WARC file version 1.0")
	settings.WarcinfoContent.Set("conformsTo", "https://iipc.github.io/warc-specifications/specifications/warc-format/warc-1.0/")

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
