package warc

import "fmt"

type DiscardHookError struct {
	URL    string
	Reason string // reason for discarding
	Err    error  // nil: discarded successfully
}

func (e *DiscardHookError) Error() string {
	return fmt.Sprintf("response was blocked by DiscardHook. url: '%s', reason: '%s', err: %v", e.URL, e.Reason, e.Err)
}

func (e *DiscardHookError) Unwrap() error {
	return e.Err
}
