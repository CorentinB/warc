package warc

import (
	"testing"

	"go.uber.org/goleak"
)

func TestMain(m *testing.M) {
	options := []goleak.Option{
		goleak.IgnoreTopFunction("internal/poll.runtime_pollWait"),
	}
	goleak.VerifyTestMain(m, options...)
}
