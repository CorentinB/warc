package warc

import (
	"io"
	"net/http"
	"net/url"
	"strings"
	"time"
)

type DedupeOptions struct {
	LocalDedupe bool
	CDXDedupe   bool
	CDXURL      string
}

type revisitRecord struct {
	responseUUID string
	targetURI    string
	date         time.Time
}

func (d *customDialer) checkLocalRevisit(digest string) revisitRecord {
	revisit, exists := d.client.dedupeHashTable.Load(digest)
	if exists {
		return revisit.(revisitRecord)
	}

	return revisitRecord{}
}

func checkCDXRevisit(CDXURL string, digest string, targetURI string) (revisitRecord, error) {
	resp, err := http.Get(CDXURL + "/web/timemap/cdx?url=" + url.QueryEscape(targetURI) + "&filter=digest:" + digest + "&limit=-1")
	if err != nil {
		return revisitRecord{}, err
	}
	defer resp.Body.Close()

	body, err := io.ReadAll(resp.Body)
	if err != nil {
		return revisitRecord{}, err
	}

	cdxReply := strings.Fields(string(body))

	if len(cdxReply) >= 7 {
		CDXDate, _ := time.Parse("20060102150405", cdxReply[1])

		return revisitRecord{
			responseUUID: "",
			targetURI:    cdxReply[2],
			date:         CDXDate,
		}, nil
	}

	return revisitRecord{}, nil
}
