package main

import (
	"bufio"
	"net/http"
	"os"
	"strconv"
	"strings"
	"sync"
	"time"

	"github.com/CorentinB/warc"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func processVerifyRecord(record *warc.Record, filepath string, results chan<- result) {
	var res result
	res.blockDigestErrorsCount, res.blockDigestValid = verifyBlockDigest(record, filepath)
	res.payloadDigestErrorsCount, res.payloadDigestValid = verifyPayloadDigest(record, filepath)
	res.warcVersionValid = verifyWarcVersion(record, filepath)
	results <- res
}

type result struct {
	warcVersionValid         bool
	blockDigestErrorsCount   int
	blockDigestValid         bool
	payloadDigestErrorsCount int
	payloadDigestValid       bool
}

func verify(cmd *cobra.Command, files []string) {
	threads, err := strconv.Atoi(cmd.Flags().Lookup("threads").Value.String())
	if err != nil {
		logrus.Fatalf("failed to parse threads: %s", err.Error())
	}

	logger := logrus.New()
	if cmd.Flags().Lookup("json").Changed {
		logger.SetFormatter(&logrus.JSONFormatter{})
	}

	for _, filepath := range files {
		startTime := time.Now()
		valid := true           // The WARC file is valid
		allRecordsRead := false // All records readed successfully
		errorsCount := 0
		recordCount := 0 // Count of records processed

		recordChan := make(chan *warc.Record, threads*2)
		results := make(chan result, threads*2)

		var processWg sync.WaitGroup
		var recordReaderWg sync.WaitGroup

		if !cmd.Flags().Lookup("json").Changed {
			// Output the message if not in --json mode
			logrus.WithFields(logrus.Fields{
				"file":    filepath,
				"threads": threads,
			}).Info("verifying")
		}
		for i := 0; i < threads; i++ {
			processWg.Add(1)
			go func() {
				defer processWg.Done()
				for record := range recordChan {
					processVerifyRecord(record, filepath, results)
				}
			}()
		}

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

		// Read records and send them to workers
		recordReaderWg.Add(1)
		go func() {
			defer recordReaderWg.Done()
			defer close(recordChan)
			for {
				record, eof, err := reader.ReadRecord()
				if eof {
					allRecordsRead = true
					break
				}
				if err != nil {
					if record == nil {
						logrus.WithFields(logrus.Fields{
							"file": filepath,
						}).Errorf("failed to read record: %v", err)
					} else {
						logrus.WithFields(logrus.Fields{
							"file":     filepath,
							"recordId": record.Header.Get("WARC-Record-ID"),
						}).Errorf("failed to read record: %v", err)
					}
					errorsCount++
					valid = false
					return
				}
				recordCount++

				// Only process Content-Type: application/http; msgtype=response (no reason to process requests or other records)
				if !strings.Contains(record.Header.Get("Content-Type"), "msgtype=response") {
					logrus.WithFields(logrus.Fields{
						"file":     filepath,
						"recordId": record.Header.Get("WARC-Record-ID"),
					}).Debugf("skipping record with Content-Type: %s", record.Header.Get("Content-Type"))
					continue
				}

				// We cannot verify the validity of Payload-Digest on revisit records yet.
				if record.Header.Get("WARC-Type") == "revisit" {
					logrus.Debugf("skipping revisit record")
					continue
				}

				recordChan <- record
			}
		}()

		// Collect results from workers

		recordReaderWg.Add(1)
		go func() {
			defer recordReaderWg.Done()
			for res := range results {
				if !res.blockDigestValid {
					valid = false
					errorsCount += res.blockDigestErrorsCount
				}
				if !res.payloadDigestValid {
					valid = false
					errorsCount += res.payloadDigestErrorsCount
				}
				if !res.warcVersionValid {
					valid = false
					errorsCount++
				}
			}
		}()

		processWg.Wait()
		close(results)
		recordReaderWg.Wait()

		if recordCount == 0 {
			logrus.Errorf("no records present in files. Nothing has been checked")
		}

		fields := logger.WithFields(logrus.Fields{
			"file":           filepath,
			"valid":          valid,
			"errors":         errorsCount,
			"count":          recordCount,
			"allRecordsRead": allRecordsRead,
		})

		// Ensure there is a visible difference when errors are present.
		if errorsCount > 0 {
			fields.Errorf("checked in %s", time.Since(startTime))
		} else {
			fields.Infof("checked in %s", time.Since(startTime))
		}

	}
}

func verifyPayloadDigest(record *warc.Record, filepath string) (errorsCount int, valid bool) {
	valid = true

	// Verify that the Payload-Digest field exists
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

	if resp.Header.Get("X-Crawler-Transfer-Encoding") != "" || resp.Header.Get("X-Crawler-Content-Encoding") != "" {
		// This header being present in the HTTP headers indicates transfer-encoding and/or content-encoding were incorrectly stripped, causing us to not be able to verify the payload digest.
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("malformed headers prevent accurate payload digest calculation")

		valid = false
		errorsCount++
		return errorsCount, valid
	}

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
		payloadDigest := warc.GetSHA256Base16(resp.Body)
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
		}).Errorf("WARC-Payload-Digest is not SHA1 or SHA256: %s", record.Header.Get("WARC-Payload-Digest"))
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
		}).Errorf("malformed WARC-Block-Digest: %s", record.Header.Get("WARC-Block-Digest"))
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
		expectedPayloadDigest := warc.GetSHA256Base16(record.Content)
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

func verifyWarcVersion(record *warc.Record, filepath string) (valid bool) {
	valid = true
	if record.Version != "WARC/1.0" && record.Version != "WARC/1.1" {
		logrus.WithFields(logrus.Fields{
			"file":     filepath,
			"recordId": record.Header.Get("WARC-Record-ID"),
		}).Errorf("invalid WARC version: %s", record.Version)
		valid = false
	}

	return valid
}
