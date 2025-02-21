package warc

import (
	"context"
	"fmt"
	"net"
	"sync"
	"time"

	"github.com/miekg/dns"
)

type cachedIP struct {
	expiresAt time.Time
	ip        net.IP
}

const maxFallbackDNSServers = 3

func (d *customDialer) archiveDNS(ctx context.Context, address string) (resolvedIP net.IP, err error) {
	// Get the address without the port if there is one
	address, _, err = net.SplitHostPort(address)
	if err != nil {
		return resolvedIP, err
	}

	// Check if the address is already an IP
	resolvedIP = net.ParseIP(address)
	if resolvedIP != nil {
		return resolvedIP, nil
	}

	// Check cache first
	if cached, ok := d.DNSRecords.Load(address); ok {
		cachedEntry := cached.(cachedIP)
		if time.Now().Before(cachedEntry.expiresAt) {
			return cachedEntry.ip, nil
		}
		// Cache entry expired, remove it
		d.DNSRecords.Delete(address)
	}

	var wg sync.WaitGroup
	var ipv4, ipv6 net.IP
	var errA, errAAAA error

	if len(d.DNSConfig.Servers) == 0 {
		return nil, fmt.Errorf("no DNS servers configured")
	}

	fallbackServers := min(maxFallbackDNSServers, len(d.DNSConfig.Servers)-1)

	for DNSServer := 0; DNSServer <= fallbackServers; DNSServer++ {
		if !d.disableIPv4 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ipv4, errA = d.lookupIP(ctx, address, dns.TypeA, DNSServer)
			}()
		}

		if !d.disableIPv6 {
			wg.Add(1)
			go func() {
				defer wg.Done()
				ipv6, errAAAA = d.lookupIP(ctx, address, dns.TypeAAAA, DNSServer)
			}()
		}

		wg.Wait()

		if errA == nil || errAAAA == nil {
			break
		}
	}
	if errA != nil && errAAAA != nil {
		return nil, fmt.Errorf("failed to resolve DNS: A error: %v, AAAA error: %v", errA, errAAAA)
	}

	// Prioritize IPv6 if both are available and enabled
	if ipv6 != nil && !d.disableIPv6 {
		resolvedIP = ipv6
	} else if ipv4 != nil && !d.disableIPv4 {
		resolvedIP = ipv4
	}

	if resolvedIP != nil {
		// Cache the result
		d.DNSRecords.Store(address, cachedIP{
			ip:        resolvedIP,
			expiresAt: time.Now().Add(d.DNSRecordsTTL),
		})
		return resolvedIP, nil
	}

	return nil, fmt.Errorf("no suitable IP address found for %s", address)
}

func (d *customDialer) lookupIP(ctx context.Context, address string, recordType uint16, DNSServer int) (net.IP, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(address), recordType)

	r, _, err := d.DNSClient.ExchangeContext(ctx, m, net.JoinHostPort(d.DNSConfig.Servers[DNSServer], d.DNSConfig.Port))
	if err != nil {
		return nil, err
	}

	// Record the DNS response
	recordTypeStr := "TYPE=A"
	if recordType == dns.TypeAAAA {
		recordTypeStr = "TYPE=AAAA"
	}

	d.client.WriteRecord(fmt.Sprintf("dns:%s?%s", address, recordTypeStr), "resource", "text/dns", r.String(), nil)

	for _, answer := range r.Answer {
		switch recordType {
		case dns.TypeA:
			if a, ok := answer.(*dns.A); ok {
				return a.A, nil
			}
		case dns.TypeAAAA:
			if aaaa, ok := answer.(*dns.AAAA); ok {
				return aaaa.AAAA, nil
			}
		}
	}

	return nil, fmt.Errorf("no %s record found", recordTypeStr)
}
