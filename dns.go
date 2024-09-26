package warc

import (
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
			return cachedEntry.ip, nil
		}
		// Cache entry expired, remove it
		d.DNSRecords.Delete(address)
	}

	var wg sync.WaitGroup
	var ipv4, ipv6 net.IP
	var errA, errAAAA error

	wg.Add(2)

	go func() {
		defer wg.Done()
		ipv4, errA = d.lookupIP(address, dns.TypeA)
	}()

	go func() {
		defer wg.Done()
		ipv6, errAAAA = d.lookupIP(address, dns.TypeAAAA)
	}()

	wg.Wait()

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

func (d *customDialer) lookupIP(address string, recordType uint16) (net.IP, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(address), recordType)

	r, _, err := d.DNSClient.Exchange(m, net.JoinHostPort(d.DNSConfig.Servers[0], d.DNSConfig.Port))
	if err != nil {
		return nil, err
	}

	// Record the DNS response
	recordTypeStr := "A"
	if recordType == dns.TypeAAAA {
		recordTypeStr = "AAAA"
	}

	d.client.WriteRecord(fmt.Sprintf("dns:%s?%s", address, recordTypeStr), "resource", "text/dns", r.String())

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
