package warc

import (
	"fmt"
	"net"

	"github.com/miekg/dns"
)

func (d *customDialer) resolveDNS(address string) (net.IP, error) {
	m := new(dns.Msg)
	m.SetQuestion(dns.Fqdn(address), dns.TypeA)

	r, _, err := d.DNSClient.Exchange(m, net.JoinHostPort(d.DNSConfig.Servers[0], d.DNSConfig.Port))
	if err != nil {
		return nil, err
	}

	// Print raw DNS output
	// fmt.Printf("Raw DNS response for %s:\n%s\n", address, r.String())

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
		return ipv6, nil
	}

	if ipv4 != nil {
		return ipv4, nil
	}

	return nil, fmt.Errorf("no suitable IP address found for %s", address)
}
