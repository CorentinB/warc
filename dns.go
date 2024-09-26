package warc

import (
	"fmt"
	"net"
	"time"

	"github.com/miekg/dns"
)

type cachedIP struct {
	expiresAt time.Time
	ip        net.IP
}

func (d *customDialer) archiveDNS(address string) (resolvedIP net.IP, err error) {
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
			return resolvedIP, nil
		}
		// Cache entry expired, remove it
		d.DNSRecords.Delete(address)
	}

	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(address), dns.TypeA)

	r, _, err := d.DNSClient.Exchange(m, net.JoinHostPort(d.DNSConfig.Servers[0], d.DNSConfig.Port))
	if err != nil {
		return resolvedIP, err
	}

	// Record the DNS response
	d.client.WriteRecord("dns:"+address, "resource", "text/dns", r.String())

	var ipv4, ipv6 net.IP

	for _, answer := range r.Answer {
		if a, ok := answer.(*dns.A); ok && !d.disableIPv4 {
			ipv4 = a.A
		} else if aaaa, ok := answer.(*dns.AAAA); ok && !d.disableIPv6 {
			ipv6 = aaaa.AAAA
			break // Prioritize IPv6 if available
		}
	}

	// Prioritize IPv6 if both are available and enabled
	if ipv6 != nil {
		resolvedIP = ipv6
	} else if ipv4 != nil {
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

	return resolvedIP, fmt.Errorf("no suitable IP address found for %s", address)
}
