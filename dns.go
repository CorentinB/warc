package warc

import (
	"context"
	"net"
)

func customResolver(ctx context.Context, network, host string) ([]net.IP, error) {
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, address string) (net.Conn, error) {
			d := net.Dialer{}
			return d.DialContext(ctx, network, address)
		},
	}

	ips, err := resolver.LookupIP(ctx, network, host)
	if err != nil {
		return nil, err
	}

	//Log DNS response
	// fmt.Printf("DNS lookup for %s: %v\n", host, ips)

	return ips, nil
}
