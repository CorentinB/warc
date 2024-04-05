package main

import (
	"time"

	"github.com/sirupsen/logrus"
)

func printExtractReport(filePath string, results map[string]int, elapsed time.Duration) {
	total := 0

	for _, v := range results {
		total += v
	}

	logrus.Infof("Processed file %s in %s", filePath, elapsed.String())
	logrus.Infof("Number of files extracted: %d", total)
	for k, v := range results {
		logrus.Infof("- %s: %d\n", k, v)
	}
}
