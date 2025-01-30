package warc

import (
	"fmt"
	"os"
	"strconv"
	"sync"
	"sync/atomic"
	"time"
)

var filenameGenerationLock = sync.Mutex{}

// GenerateWarcFileName generate a WARC file name following recommendations
// of the specs:
// Prefix-Timestamp-Serial-Crawlhost.warc.gz
func generateWarcFileName(prefix string, compression string, serial *atomic.Uint64) (fileName string) {
	filenameGenerationLock.Lock()
	defer filenameGenerationLock.Unlock()

	// Get host name as reported by the kernel
	hostName, err := os.Hostname()
	if err != nil {
		panic(err)
	}

	// Don't let serial overflow past 99999, the current maximum with 5 serial digits.
	serial.CompareAndSwap(99999, 0)

	// Atomically increase the global serial number
	serial.Add(1)

	formattedSerial := formatSerial(serial, "5")

	now := time.Now().UTC()
	date := now.Format("20060102150405") + strconv.Itoa(now.Nanosecond())[:3]

	var fileExt string
	if compression == "GZIP" {
		fileExt = ".warc.gz.open"
	} else if compression == "ZSTD" {
		fileExt = ".warc.zst.open"
	} else {
		fileExt = ".warc.open"
	}

	return prefix + "-" + date + "-" + formattedSerial + "-" + hostName + fileExt
}

// formatSerial add the correct padding to the serial
// E.g. with serial = 23 and format = 5:
// formatSerial return 00023
func formatSerial(serial *atomic.Uint64, format string) string {
	return fmt.Sprintf("%0"+format+"d", serial.Load())
}

// isFielSizeExceeded compare the size of a file (filePath) with
// a max size (maxSize), if the size of filePath exceed maxSize,
// it returns true, else, it returns false
func isFileSizeExceeded(file *os.File, maxSize float64) bool {
	// Get actual file size
	stat, err := file.Stat()
	if err != nil {
		panic(err)
	}
	fileSize := (float64)((stat.Size() / 1024) / 1024)

	// If fileSize exceed maxSize, return true
	return fileSize >= maxSize
}
