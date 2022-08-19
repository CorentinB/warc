package warc

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	// goleak.IgnoreTopFunction("github.com/CorentinB/warc.TestHTTPClientWithProxy")
	goleak.VerifyTestMain(m)
}
