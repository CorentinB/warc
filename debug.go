package warc

import (
	"encoding/json"
	"io/ioutil"
	"path"
	"strconv"
	"time"
)

type DebugFile struct {
	Time string  `json:"time"`
	Err  error   `json:"err"`
	Data *Record `json:"data"`
}

func (httpClient *CustomHTTPClient) writeDebugFile(record *Record, err error) {
	filename := strconv.Itoa(int(time.Now().UTC().Unix())) + RandomString(11) + ".json"

	debugData, err := json.MarshalIndent(DebugFile{
		Time: time.Now().UTC().Format(time.RFC3339),
		Err:  err,
		Data: record,
	}, "", "\t")
	if err != nil {
		panic(err)
	}

	err = ioutil.WriteFile(path.Join(httpClient.debugDir, filename), debugData, 0644)
	if err != nil {
		panic(err)
	}
}
