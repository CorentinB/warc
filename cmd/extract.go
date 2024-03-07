package main

import (
	"bufio"
	"io"
	"mime"
	"net/http"
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
			go processRecord(cmd, record, &swg)
		}

		swg.Wait()
		logrus.Infof("finished processing %s in %s", filepath, time.Since(startTime))
	}
}

func processRecord(cmd *cobra.Command, record *warc.Record, swg *sizedwaitgroup.SizedWaitGroup) {
	defer record.Content.Close()
	defer swg.Done()

	// Only process Content-Type: application/http; msgtype=response (no reason to process requests)
	if record.Header.Get("Content-Type") != "application/http; msgtype=response" {
		logrus.Debugf("skipping record with Content-Type: %s", record.Header.Get("Content-Type"))
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

	if slices.Contains(contentTypesToExtract, response.Header.Get("Content-Type")) {
		err = writeFile(cmd, response, record)
		if err != nil {
			logrus.Errorf("failed to write file: %v", err)
			return
		}
	}
}

func writeFile(vmd *cobra.Command, resp *http.Response, record *warc.Record) error {
	// Find the filename either from the Content-Disposition header or the last part of the URL
	filename := path.Base(record.Header.Get("WARC-Target-URI"))

	if resp.Header.Get("Content-Disposition") != "" {
		_, params, err := mime.ParseMediaType(resp.Header.Get("Content-Disposition"))
		if err != nil {
			return err
		}

		filename = params["filename"]
	}

	// Check if the file already exists
	outputDir := vmd.Flags().Lookup("output").Value.String()
	outputPath := path.Join(outputDir, filename)
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
	}

	// Create the file
	file, err := os.OpenFile(outputPath, os.O_CREATE|os.O_WRONLY, 0644)
	if err != nil {
		return err
	}
	defer file.Close()

	// Write the response body to the file
	_, err = io.Copy(file, resp.Body)
	if err != nil {
		return err
	}

	return nil
}
