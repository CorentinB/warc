package warc

import (
	"context"
	"errors"
	"os"
	"sync"
	"testing"
	"time"

	"github.com/miekg/dns"
)

const (
	invalidDNS = "198.51.100.0"
	publicDNS  = "8.8.8.8"

	nxdomain   = "warc.faketld:443"
	targetHost = "www.google.com"
	target     = "www.google.com:443"
)

func newTestCustomDialer() (d *customDialer) {
	d = new(customDialer)

	d.DNSRecords = new(sync.Map)
	d.DNSConfig = &dns.ClientConfig{
		Port: "53",
	}
	d.DNSClient = &dns.Client{
		Timeout: 2 * time.Second,
	}

	return d
}

func setup(t *testing.T) (*customDialer, *CustomHTTPClient, func()) {
	var (
		rotatorSettings = NewRotatorSettings()
		err             error
	)
	rotatorSettings.OutputDirectory, err = os.MkdirTemp("", "warc-tests-")
	if err != nil {
		t.Fatal(err)
	}

	rotatorSettings.Prefix = "TEST-DNS"

	httpClient, err := NewWARCWritingHTTPClient(HTTPClientSettings{
		RotatorSettings: rotatorSettings,
	})
	if err != nil {
		t.Fatalf("Unable to init WARC writing HTTP client: %s", err)
	}

	d := newTestCustomDialer()
	d.client = httpClient
	d.DNSRecordsTTL = time.Second

	cleanup := func() {
		err = httpClient.Close()
		if err != nil {
			panic(err)
		}
		os.RemoveAll(rotatorSettings.OutputDirectory)
	}

	return d, httpClient, cleanup
}

func TestNoDNSServersConfigured(t *testing.T) {
	d, _, cleanup := setup(t)
	defer cleanup()

	wantErr := errors.New("no DNS servers configured")
	d.DNSConfig.Servers = []string{}
	_, err := d.archiveDNS(context.Background(), target)
	if err.Error() != wantErr.Error() {
		t.Errorf("Want error %s, got %s", wantErr, err)
	}
}

func TestNormalDNSResolution(t *testing.T) {
	d, _, cleanup := setup(t)
	defer cleanup()

	d.DNSConfig.Servers = []string{publicDNS}
	IP, err := d.archiveDNS(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}

	loaded, ok := d.DNSRecords.Load(targetHost)
	if !ok {
		t.Error("Cache not working")
	}
	cached := loaded.(cachedIP)
	if cached.ip.String() != IP.String() {
		t.Error("Cached IP not matching resolved IP")
	}
}

func TestIPv6Only(t *testing.T) {
	d, _, cleanup := setup(t)
	defer cleanup()

	d.disableIPv4 = true
	d.disableIPv6 = false

	d.DNSConfig.Servers = []string{publicDNS}
	IP, err := d.archiveDNS(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Resolved IP: %s", IP)
}

func TestNXDOMAIN(t *testing.T) {
	d, _, cleanup := setup(t)
	defer cleanup()

	IP, err := d.archiveDNS(context.Background(), nxdomain)
	if err == nil {
		t.Error("Want failure,", "got resolved IP", IP)
	}
}

func TestDNSFallback(t *testing.T) {
	d, _, cleanup := setup(t)
	defer cleanup()

	d.DNSRecords.Delete(targetHost)
	d.DNSConfig.Servers = []string{invalidDNS, publicDNS}
	IP, err := d.archiveDNS(context.Background(), target)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("Resolved IP: %s", IP)
}
