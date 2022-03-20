package warc

// Header provides information about the WARC record. It stores WARC record
// field names and their values. Since WARC field names are case-insensitive,
// the Header methods are case-insensitive as well.
type Header map[string]string

// Set sets the header field associated with key to value.
func (h Header) Set(key, value string) {
	h[key] = value
}

// Get returns the value associated with the given key.
// If there is no value associated with the key, Get returns "".
func (h Header) Get(key string) string {
	return h[key]
}

// Del deletes the value associated with key.
func (h Header) Del(key string) {
	delete(h, key)
}

// NewHeader creates a new WARC header.
func NewHeader() Header {
	return make(map[string]string)
}
