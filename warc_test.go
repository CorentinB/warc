package warc

import (
	"bytes"
	"io/ioutil"
	"log"
	"net/http"
	"net/url"
	"reflect"
	"testing"
)

var testHTTPResponse = &http.Response{
	Body: ioutil.NopCloser(bytes.NewBufferString("Is archiving awesome?")),
	Request: &http.Request{
		Header: http.Header{
			"Host": []string{"localhost"},
		},
		Body: ioutil.NopCloser(bytes.NewBufferString("Yes it is!")),
		URL: &url.URL{
			Host:   "localhost",
			Scheme: "http",
		},
	},
}

var testResponseContent = []byte{72, 84, 84, 80, 47, 48, 46, 48, 32, 48, 48, 48,
	32, 115, 116, 97, 116, 117, 115, 32, 99, 111, 100, 101, 32, 48, 13, 10, 13,
	10, 73, 115, 32, 97, 114, 99, 104, 105, 118, 105, 110, 103, 32, 97, 119,
	101, 115, 111, 109, 101, 63}
var testRequestContent = []byte{71, 69, 84, 32, 47, 32, 72, 84, 84, 80, 47, 49,
	46, 49, 13, 10, 72, 111, 115, 116, 58, 32, 108, 111, 99, 97, 108, 104, 111,
	115, 116, 13, 10, 85, 115, 101, 114, 45, 65, 103, 101, 110, 116, 58, 32,
	71, 111, 45, 104, 116, 116, 112, 45, 99, 108, 105, 101, 110, 116, 47, 49,
	46, 49, 13, 10, 84, 114, 97, 110, 115, 102, 101, 114, 45, 69, 110, 99, 111,
	100, 105, 110, 103, 58, 32, 99, 104, 117, 110, 107, 101, 100, 13, 10, 65,
	99, 99, 101, 112, 116, 45, 69, 110, 99, 111, 100, 105, 110, 103, 58, 32,
	103, 122, 105, 112, 13, 10, 13, 10, 49, 13, 10, 89, 13, 10, 57, 13, 10,
	101, 115, 32, 105, 116, 32, 105, 115, 33, 13, 10, 48, 13, 10, 13, 10}

var testResponseSHA1 = "sha1:I7T3S4A2ES6FQNDW2CW5GUGAUUOWLIZ3"
var testRequestSHA1 = "sha1:KCAQUL5WHF47633LXN2CFZUWH6ECXVQL"

// Tests for the RecordsFrumHTTPResponse function
func TestRecordsFromHTTPResponse(t *testing.T) {
	records, err := RecordsFromHTTPResponse(testHTTPResponse)
	if err != nil {
		t.Error("Failed to turn a http.Response into a RecordBatch, err: " + err.Error())
	}

	responseContent, err := ioutil.ReadAll(records.Records[0].Content)
	if err != nil {
		log.Fatal(err)
	}

	if reflect.DeepEqual(responseContent, testResponseContent) == false ||
		records.Records[0].Header.Get("WARC-Payload-Digest") != testResponseSHA1 {
		t.Error("Failed to turn a http.Response into a WARC record")
	}

	requestContent, err := ioutil.ReadAll(records.Records[1].Content)
	if err != nil {
		log.Fatal(err)
	}

	if reflect.DeepEqual(requestContent, testRequestContent) == false ||
		records.Records[1].Header.Get("WARC-Payload-Digest") != testRequestSHA1 {
		t.Error("Failed to turn a http.Response.Request into a WARC record")
	}
}
