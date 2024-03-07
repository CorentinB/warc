package main

import (
	"os"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(extractCmd)
	extractCmd.Flags().IntP("threads", "t", 1, "Number of threads to use for extraction")
	extractCmd.Flags().StringP("output", "o", "output", "Output directory for extracted files")
	extractCmd.Flags().StringSliceP("content-type", "c", []string{}, "Content type that should be extracted")
	extractCmd.Flags().Bool("allow-overwrite", false, "Allow overwriting of existing files")
}

// rootCmd represents the base command when called without any subcommands
var rootCmd = &cobra.Command{
	Use:   "cmd",
	Short: "Utility to process WARC files",
	Long:  `Utility to process WARC files`,
}

var extractCmd = &cobra.Command{
	Use:   "extract",
	Short: "Extracts the URLs from one or many WARC file(s)",
	Long:  `Extracts the URLs from one or many WARC file(s)`,
	Args:  cobra.MinimumNArgs(1),
	Run:   extract,
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
