package main

import (
	"flag"
	"github.com/slyrz/warc"
	"io"
	"log"
	"os"
	"strings"
	"fmt"
)

func filterWarc(filename string, filterKey string, filterVal string) {
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
			log.Println("filtered in",
				record.Header.Get("warc-type"),
				record.Header.Get("warc-target-uri"))
		} else {
			log.Println("filtered out",
				record.Header.Get("warc-type"),
				record.Header.Get("warc-target-uri"))
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
		fmt.Fprintf(os.Stderr, "\tFILTER_SPEC is of the form HEADER:VALUE where HEADER is a warc record header key\n")
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
		filterKey := strings.TrimSpace(filterKeyVal[0])
		filterVal := strings.TrimSpace(filterKeyVal[1])
		for i := 1; i < flag.NArg(); i++ {
			filterWarc(flag.Arg(i), filterKey, filterVal)
		}
	}
}
