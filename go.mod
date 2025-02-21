module github.com/CorentinB/warc

go 1.24

require (
	github.com/armon/go-socks5 v0.0.0-20160902184237-e75332964ef5
	github.com/google/uuid v1.6.0
	github.com/klauspost/compress v1.18.0
	github.com/miekg/dns v1.1.63
	github.com/paulbellamy/ratecounter v0.2.0
	github.com/refraction-networking/utls v1.6.7
	github.com/remeh/sizedwaitgroup v1.0.0
	github.com/spf13/cobra v1.9.1
	github.com/ulikunitz/xz v0.5.12
	go.uber.org/goleak v1.3.0
	golang.org/x/net v0.35.0
	golang.org/x/sync v0.11.0
)

require (
	github.com/andybalholm/brotli v1.1.1 // indirect
	github.com/cloudflare/circl v1.6.0 // indirect
	github.com/inconshreveable/mousetrap v1.1.0 // indirect
	github.com/spf13/pflag v1.0.6 // indirect
	golang.org/x/crypto v0.33.0 // indirect
	golang.org/x/mod v0.23.0 // indirect
	golang.org/x/sys v0.30.0 // indirect
	golang.org/x/tools v0.30.0 // indirect
)

// Unsure exactly where these versions came from, but no longer exist. If we plan to publish under these versions, we need to remove them from this retract list.
retract (
	v1.1.2
	v1.1.0
	v1.0.0
)
