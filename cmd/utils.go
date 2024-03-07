package main

import (
	"time"

	"github.com/sirupsen/logrus"
)

func printExtractReport(fileCount int, results map[string]int, elapsed time.Duration) {
	total := 0

	for _, v := range results {
		total += v
	}

	logrus.Infof("Processed %d file(s) in %s", fileCount, elapsed.String())
	logrus.Infof("Number of files extracted: %d", total)
	for k, v := range results {
		logrus.Infof("- %s: %d\n", k, v)
	}
}
