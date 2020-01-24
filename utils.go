package warc

import (
	"context"
	"crypto/sha1"
	"encoding/base32"
	"fmt"
	"io"
	"os"
	"os/signal"
	"sync/atomic"
	"time"
)

// GetSHA1 return the SHA1 of a []byte,
// can be used to fill the WARC-Payload-Digest header
func GetSHA1(content []byte) string {
	sha := sha1.New()
	sha.Write(content)

	return base32.StdEncoding.EncodeToString(sha.Sum(nil))
}

// NewWriter creates a new WARC writer.
func NewWriter(writer io.Writer, fileName string) *Writer {
	return &Writer{
		FileName: fileName,
		target:   writer,
	}
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

// NewExchange creates a new pair of request/response
// records, and set the time
func NewExchange() *Exchange {
	return &Exchange{
		Response:    NewRecord(),
		Request:     NewRecord(),
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
		Encryption:      false,
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
func generateWarcFileName(prefix string, encryption bool, serial int) (fileName string) {
	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	formattedSerial := formatSerial(serial, "5")

	if encryption {
		return prefix + "-" + time.Now().UTC().Format("20060102150405") + "-" + formattedSerial + "-" + hostName + ".warc.gz.open"
	}
	return prefix + "-" + time.Now().UTC().Format("20060102150405") + "-" + formattedSerial + "-" + hostName + ".warc.open"
}

func listenCtrlC(cancel context.CancelFunc) {
	c := make(chan os.Signal, 1)
	signal.Notify(c, os.Interrupt)
	<-c
	atomic.StoreInt32(&exitRequested, 1)
	cancel()
	fmt.Fprintln(os.Stderr, "\nWaiting for WARC writing to finish...")
	fmt.Fprintln(os.Stderr, "Press ^C again to exit instantly.")
	<-c
	fmt.Fprintln(os.Stderr, "\nKilled!")
	os.Exit(130)
}
