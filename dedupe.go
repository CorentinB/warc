package warc

import (
	"io"
	"net/http"
	"net/url"
	"strings"
)

type DedupeOptions struct {
	LocalDedupe   bool
	CDXDedupe     bool
	CDXURL        string
	SizeThreshold int
}

type revisitRecord struct {
	responseUUID string
	targetURI    string
	date         string
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
		return revisitRecord{
			responseUUID: "",
			targetURI:    cdxReply[2],
			date:         cdxReply[1],
		}, nil
	}

	return revisitRecord{}, nil
}
