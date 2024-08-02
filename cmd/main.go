package main

import (
	"os"
	"runtime"

	"github.com/spf13/cobra"
)

func init() {
	rootCmd.AddCommand(extractCmd)
	rootCmd.AddCommand(verifyCmd)

	extractCmd.Flags().IntP("threads", "t", 1, "Number of threads to use for extraction")
	extractCmd.Flags().StringP("output", "o", "output", "Output directory for extracted files")
	extractCmd.Flags().StringSliceP("content-type", "c", []string{}, "Content type that should be extracted")
	extractCmd.Flags().Bool("allow-overwrite", false, "Allow overwriting of existing files")
	extractCmd.Flags().Bool("host-sort", false, "Sort the extracted URLs by host")
	extractCmd.Flags().Bool("hash-suffix", false, "When duplicate file names exist, the hash will be added if a duplicate file name exists. ")

	verifyCmd.Flags().IntP("threads", "t", runtime.NumCPU(), "Number of threads to use for verification")
	verifyCmd.Flags().Bool("json", false, "Output results in JSON format")
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

var verifyCmd = &cobra.Command{
	Use:   "verify",
	Short: "Verify the validity of one or many WARC file(s)",
	Long:  `Verify the validity of xtracts the URLs from one or many WARC file(s)`,
	Args:  cobra.MinimumNArgs(1),
	Run:   verify,
}

func main() {
	err := rootCmd.Execute()
	if err != nil {
		os.Exit(1)
	}
}
