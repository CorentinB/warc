package main

import (
	"fmt"
	"log/slog"
	"time"
)

func printExtractReport(filePath string, results map[string]int, elapsed time.Duration) {
	total := 0

	for _, v := range results {
		total += v
	}

	slog.Info(fmt.Sprintf("Processed file %s in %s", filePath, elapsed.String()))
	slog.Info(fmt.Sprintf("Number of files extracted: %d", total))
	for k, v := range results {
		slog.Info(fmt.Sprintf("- %s: %d\n", k, v))
	}
}
