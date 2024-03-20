package main

import (
	"bufio"
	"compress/gzip"
	"io"
	"mime"
	"net/http"
	"net/url"
	"os"
	"path"
	"slices"
	"strconv"
	"strings"
	"time"

	"github.com/CorentinB/warc"
	"github.com/remeh/sizedwaitgroup"
	"github.com/sirupsen/logrus"
	"github.com/spf13/cobra"
)

func extract(cmd *cobra.Command, files []string) {
	threads, err := strconv.Atoi(cmd.Flags().Lookup("threads").Value.String())
	if err != nil {
		logrus.Fatalf("failed to parse threads: %s", err.Error())
	}

	swg := sizedwaitgroup.New(threads)

	for _, filepath := range files {
		startTime := time.Now()
		resultsChan := make(chan string)
		results := make(map[string]int)

		f, err := os.Open(filepath)
		if err != nil {
			logrus.Errorf("failed to open file: %s", err.Error())
			return
		}

		reader, err := warc.NewReader(f)
		if err != nil {
			logrus.Errorf("warc.NewReader failed for %q: %v", filepath, err)
			return
		}

		go func(c chan string) {
			for result := range c {
				results[result]++
			}
		}(resultsChan)

		for {
			record, err := reader.ReadRecord()
			if err != nil {
				if err != io.EOF {
					logrus.Errorf("failed to read all record content: %v", err)
					return
				}
				break
			}

			swg.Add()
			go processRecord(cmd, record, &resultsChan, &swg)
		}

		swg.Wait()
		close(resultsChan)

		printExtractReport(len(files), results, time.Since(startTime))
	}
}

func processRecord(cmd *cobra.Command, record *warc.Record, resultsChan *chan string, swg *sizedwaitgroup.SizedWaitGroup) {
	defer record.Content.Close()
	defer swg.Done()

	// Only process Content-Type: application/http; msgtype=response (no reason to process requests or other records)
	if !strings.Contains(record.Header.Get("Content-Type"), "msgtype=response") {
		logrus.Debugf("skipping record with Content-Type: %s", record.Header.Get("Content-Type"))
		return
	}

	if record.Header.Get("WARC-Type") == "revisit" {
		logrus.Debugf("skipping revisit record.")
		return
	}

	// Read the entire record.Content into a bufio.Reader
	response, err := http.ReadResponse(bufio.NewReader(record.Content), nil)
	if err != nil {
		logrus.Errorf("failed to read response: %v", err)
		return
	}

	// If the response's Content-Type match one of the content types to extract, write the file
	contentTypesToExtract := strings.Split(strings.Trim(cmd.Flags().Lookup("content-type").Value.String(), "[]"), ",")

	if slices.ContainsFunc(contentTypesToExtract, func(s string) bool {
		return strings.Contains(response.Header.Get("Content-Type"), s)
	}) {
		err = writeFile(cmd, response, record)
		if err != nil {
			logrus.Errorf("failed to write file: %v", err)
			return
		}

		// Send the result to the results channel
		*resultsChan <- response.Header.Get("Content-Type")
	}
}

func writeFile(vmd *cobra.Command, resp *http.Response, record *warc.Record) error {
	// Find the filename either from the Content-Disposition header or the last part of the URL
	filename := path.Base(record.Header.Get("WARC-Target-URI"))

	url, err := url.Parse(record.Header.Get("WARC-Target-URI"))
	if err != nil {
		return err
	}

	if resp.Header.Get("Content-Disposition") != "" {
		_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
		if err != nil {
			return err
		}

		filename = params["filename"]
	}

	// Check if the file already exists
	outputDir := vmd.Flags().Lookup("output").Value.String()
	domainPath := path.Join(outputDir, url.Host)
	outputPath := path.Join(outputDir, url.Host, filename)
	if _, err := os.Stat(outputPath); err == nil {
		if !vmd.Flags().Lookup("allow-overwrite").Changed {
			logrus.Infof("file %s already exists, skipping", filename)
			return nil
		}
	}

	// Create the output directory if it doesn't exist
	if _, err := os.Stat(outputPath); os.IsNotExist(err) {
		err := os.MkdirAll(outputDir, 0755)
		if err != nil {
			return err
		}

		err = os.MkdirAll(domainPath, 0755)
		if err != nil {
			return err
		}
	}

	// Create the file
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Close body when finished.
	defer resp.Body.Close()

	var reader io.ReadCloser

	switch resp.Header.Get("Content-Encoding") {
	case "gzip":
		reader, err = gzip.NewReader(resp.Body)
		if err != nil {
			return err
		}
		defer reader.Close()
	default:
		reader = resp.Body
	}

	// Write the response body to the file
	_, err = io.Copy(file, reader)
	if err != nil {
		return err
	}

	return nil
}
