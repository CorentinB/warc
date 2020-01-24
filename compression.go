package warc

import (
	"bufio"
	"compress/bzip2"
	"compress/gzip"
	"io"
	"io/ioutil"
)

const (
	CompressionNone CompressionType = iota
	CompressionBZIP
	CompressionGZIP
)

type CompressionType int

func (c CompressionType) String() string {
	switch c {
	case CompressionNone:
		return "CompressionNone"
	case CompressionBZIP:
		return "CompressionGZIP"
	case CompressionGZIP:
		return "CompressionGZIP"
	}
	return ""
}

func (r *Reader) Compression() CompressionType {
	return r.compression
}

// guessCompression returns the compression type of a data stream by matching
// the first two bytes with the magic numbers of compression formats.
func guessCompression(b *bufio.Reader) (CompressionType, error) {
	magic, err := b.Peek(2)
	if err != nil {
		if err == io.EOF {
			err = nil
		}
		return CompressionNone, err
	}
	switch {
	case magic[0] == 0x42 && magic[1] == 0x5a:
		return CompressionBZIP, nil
	case magic[0] == 0x1f && magic[1] == 0x8b:
		return CompressionGZIP, nil
	}
	return CompressionNone, nil
}

// decompress automatically decompresses data streams and makes sure the result
// obeys the io.ReadCloser interface. This way callers don't need to check
// whether the underlying reader has a Close() function or not, they just call
// defer Close() on the result.
func decompress(r io.Reader) (compr CompressionType, res io.ReadCloser, err error) {
	// Create a buffered reader to peek the stream's magic number.
	dataReader := bufio.NewReader(r)
	compr, err = guessCompression(dataReader)
	if err != nil {
		return CompressionNone, nil, err
	}
	switch compr {
	case CompressionGZIP:
		gzipReader, err := gzip.NewReader(dataReader)
		if err != nil {
			return CompressionNone, nil, err
		}
		res = gzipReader
	case CompressionBZIP:
		bzipReader := bzip2.NewReader(dataReader)
		res = ioutil.NopCloser(bzipReader)
	case CompressionNone:
		res = ioutil.NopCloser(dataReader)
	}
	return compr, res, err
}
