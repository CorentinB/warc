package main

import (
	"flag"
	"github.com/slyrz/warc"
	"io"
	"log"
	"os"
	"strings"
	"fmt"
	// "compress/gzip"
)

func filterWarc(writer *warc.Writer, filename string, filterKey string,
	filterVal string) {
	file, err := os.Open(filename)
	if err != nil {
		log.Fatalln(err, "opening", filename)
	}
	defer file.Close()
	reader, err := warc.NewReaderMode(file, warc.SequentialMode)
	if err != nil {
		log.Fatalln(err, "creating warc reader for", filename)
	}
	for {
		record, err := reader.ReadRecord()
		if err == io.EOF {
			break // end of warc
		}
		if err != nil {
			log.Fatalln(err, "reading record from", filename)
		}
		if record.Header.Get(filterKey) == filterVal {
			_, err := writer.WriteRecord(record)
			if err != nil {
				log.Fatalln(err, "writing record", record)
			}
		}
	}
}

func main() {
	flag.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s SUBCOMMAND [arguments]\n", os.Args[0])
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Subcommands:\n")
		fmt.Fprintf(os.Stderr, "\tfilter: filter input warcs to create new warcs\n")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Use %s SUBCOMMAND --help for more information about a command\n", os.Args[0])
		flag.PrintDefaults()
	}
	filterCommand := flag.NewFlagSet("filter", flag.ExitOnError)
	filterCommand.Usage = func() {
		fmt.Fprintf(os.Stderr, "Usage: %s filter FILTER_SPEC WARCFILE...\n", os.Args[0])
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Filters input warcs to create new warcs on stdout\n")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Positional arguments:\n")
		fmt.Fprintf(os.Stderr, "\tFILTER_SPEC\tspecifies which records to include in output;\n\t\t\tform is HEADER:VALUE where HEADER is a warc record header key\n")
		fmt.Fprintf(os.Stderr, "\tWARCFILE...\twarc files to filter\n")
		fmt.Fprintln(os.Stderr)
		fmt.Fprintf(os.Stderr, "Examples:\n")
		fmt.Fprintf(os.Stderr, "\t%s filter warc-type:response foo.warc.gz bar.warc.gz\n", os.Args[0])
		fmt.Fprintf(os.Stderr, "\t\tWrite response records from the input files to stdout\n")
	}
	if len(os.Args) < 2 {
		flag.Usage()
		os.Exit(1)
	}
	switch os.Args[1] {
	case "filter":
		filterCommand.Parse(os.Args[2:])
	default:
		flag.Usage()
		os.Exit(1)
	}

	if filterCommand.Parsed() {
		if filterCommand.NArg() < 2 {
			filterCommand.Usage()
			os.Exit(1)
		}
		filterKeyVal := strings.SplitN(filterCommand.Arg(0), ":", 2)
		if len(filterKeyVal) != 2 {
			filterCommand.Usage()
			fmt.Fprintln(os.Stderr)
			fmt.Fprintf(os.Stderr,
				"%s filter: Error: Bad filter spec: %s\n",
				os.Args[0], filterCommand.Arg(1))
			os.Exit(1)
		}

		filterKey := strings.TrimSpace(filterKeyVal[0])
		filterVal := strings.TrimSpace(filterKeyVal[1])

		// XXX this would not be record-at-a-time
		// stdoutGz := gzip.NewWriter(os.Stdout)
		// defer func() {
		// 	err := stdoutGz.Close()
		// 	if err != nil {
		// 		log.Fatalln(err, "finalizing gzip")
		// 	}
		// }()
		// writer := warc.NewWriter(stdoutGz)

		writer := warc.NewWriter(os.Stdout)
		for i := 1; i < filterCommand.NArg(); i++ {
			filterWarc(writer, filterCommand.Arg(i),
				filterKey, filterVal)
		}
	}
}
