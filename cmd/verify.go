package main

import (
	"bufio"
	"io"
	"net/http"
	"os"
	"strings"
	"time"

	"github.com/CorentinB/warc"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func verify(cmd *cobra.Command, files []string) {
	// threads, err := strconv.Atoi(cmd.Flags().Lookup("threads").Value.String())
	// if err != nil {
	// 	logrus.Fatalf("failed to parse threads: %s", err.Error())
	// }

	logger := logrus.New()
	if cmd.Flags().Lookup("json").Changed {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	for _, filepath := range files {
		startTime := time.Now()
		valid := true
		errorsCount := 0

		f, err := os.Open(filepath)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"file": filepath,
			}).Errorf("failed to open file: %v", err)
			return
		}

		reader, err := warc.NewReader(f)
		if err != nil {
			logrus.WithFields(logrus.Fields{
				"file": filepath,
			}).Errorf("warc.NewReader failed: %v", err)
			return
		}

		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if err != io.EOF {
					logrus.WithFields(logrus.Fields{
						"file":     filepath,
						"recordId": record.Header.Get("WARC-Record-ID"),
					}).Errorf("failed to read all record content: %v", err)
					return
				}
				break
			}

			// Only process Content-Type: application/http; msgtype=response (no reason to process requests or other records)
			if !strings.Contains(record.Header.Get("Content-Type"), "msgtype=response") {
				logrus.WithFields(logrus.Fields{
					"file":     filepath,
					"recordId": record.Header.Get("WARC-Record-ID"),
				}).Debugf("skipping record with Content-Type: %s", record.Header.Get("Content-Type"))
				continue
			}

			blockDigestErrorsCount, blockDigestValid := verifyBlockDigest(record, filepath)
			errorsCount += blockDigestErrorsCount
			if !blockDigestValid {
				valid = false
			}

			payloadDigestErrorsCount, payloadDigestValid := verifyPayloadDigest(record, filepath)
			errorsCount += payloadDigestErrorsCount
			if !payloadDigestValid {
				valid = false
			}
		}

		logger.WithFields(logrus.Fields{
			"file":   filepath,
			"valid":  valid,
			"errors": errorsCount,
		}).Infof("verified in %s", time.Since(startTime))
	}
}

func verifyPayloadDigest(record *warc.Record, filepath string) (errorsCount int, valid bool) {
	valid = true

	// Verify that the Payload-Digest is SHA1
	if record.Header.Get("WARC-Payload-Digest") == "" {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("WARC-Payload-Digest is missing")
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	payloadDigestSplitted := strings.Split(record.Header.Get("WARC-Payload-Digest"), ":")
	if len(payloadDigestSplitted) != 2 {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("Malformed WARC-Payload-Digest: %s", record.Header.Get("WARC-Payload-Digest"))
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	// Calculate expected WARC-Payload-Digest
	_, err := record.Content.Seek(0, 0)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("failed to seek record content: %v", err)
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	resp, err := http.ReadResponse(bufio.NewReader(record.Content), nil)
	if err != nil {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("failed to read response: %v", err)
		valid = false
		errorsCount++
		return errorsCount, valid
	}
	defer resp.Body.Close()
	defer record.Content.Seek(0, 0)

	if payloadDigestSplitted[0] == "sha1" {
		payloadDigest := warc.GetSHA1(resp.Body)
		if payloadDigest == "ERROR" {
			logrus.WithFields(logrus.Fields{
				"file":     filepath,
				"recordId": record.Header.Get("WARC-Record-ID"),
			}).Errorf("failed to calculate payload digest")
			valid = false
			errorsCount++
			return errorsCount, valid
		}
	} else if payloadDigestSplitted[0] == "sha256" {
		payloadDigest := warc.GetSHA256(resp.Body)
		if payloadDigest == "ERROR" {
			logrus.WithFields(logrus.Fields{
				"file":     filepath,
				"recordId": record.Header.Get("WARC-Record-ID"),
			}).Errorf("failed to calculate payload digest")
			valid = false
			errorsCount++
			return errorsCount, valid
		}
	} else {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("WARC-Payload-Digest is not sha1: %s", record.Header.Get("WARC-Payload-Digest"))
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	return errorsCount, valid
}

func verifyBlockDigest(record *warc.Record, filepath string) (errorsCount int, valid bool) {
	valid = true

	// Verify that the WARC-Block-Digest is sha1
	if record.Header.Get("WARC-Block-Digest") == "" {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("WARC-Block-Digest is missing")
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	blockDigestSplitted := strings.Split(record.Header.Get("WARC-Block-Digest"), ":")
	if len(blockDigestSplitted) != 2 {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("Malformed WARC-Block-Digest: %s", record.Header.Get("WARC-Block-Digest"))
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	defer record.Content.Seek(0, 0)

	if blockDigestSplitted[0] == "sha1" {
		expectedPayloadDigest := warc.GetSHA1(record.Content)
		if expectedPayloadDigest != blockDigestSplitted[1] {
			logrus.WithFields(logrus.Fields{
				"file":     filepath,
				"recordId": record.Header.Get("WARC-Record-ID"),
			}).Errorf("WARC-Block-Digest mismatch: expected %s, got %s", expectedPayloadDigest, blockDigestSplitted[1])
			valid = false
			errorsCount++
		}
	} else if blockDigestSplitted[0] == "sha256" {
		expectedPayloadDigest := warc.GetSHA256(record.Content)
		if expectedPayloadDigest != blockDigestSplitted[1] {
			logrus.WithFields(logrus.Fields{
				"file":     filepath,
				"recordId": record.Header.Get("WARC-Record-ID"),
			}).Errorf("WARC-Block-Digest mismatch: expected %s, got %s", expectedPayloadDigest, blockDigestSplitted[1])
			valid = false
			errorsCount++
		}
	} else {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("WARC-Block-Digest is not sha1: %s", record.Header.Get("WARC-Block-Digest"))
		valid = false
		errorsCount++
		return errorsCount, valid
	}

	return errorsCount, valid
}
