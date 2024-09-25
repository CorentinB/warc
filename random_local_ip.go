package warc

import (
	"context"
	"fmt"
	"net"
	"strings"
	"sync"
	"sync/atomic"
	"time"
)

var (
	IPv6 *availableIPs
	IPv4 *availableIPs
)

type availableIPs struct {
	IPs []net.IP
	sync.Mutex
	Index uint32
}

func (c *CustomHTTPClient) getAvailableIPs() (IPs []net.IP, err error) {
	var first = true

	if IPv6 == nil {
		IPv6 = &availableIPs{}
	}

	if IPv4 == nil {
		IPv4 = &availableIPs{}
	}

	for {
		select {
		case <-c.interfacesWatcherStop:
			return nil, nil
		default:
			// Get all network interfaces
			interfaces, err := net.Interfaces()
			if err != nil {
				time.Sleep(time.Second)
				continue
			}

			// Iterate over the interfaces
			var newIPv4 []net.IP
			var newIPv6 []net.IP
			for _, iface := range interfaces {
				if strings.Contains(iface.Name, "docker") {
					continue
				}

				// Get the addresses associated with the interface
				addrs, err := iface.Addrs()
				if err != nil {
					continue
				}

				// Iterate over the addresses
				for _, addr := range addrs {
					ipNet, ok := addr.(*net.IPNet)
					if ok && !ipNet.IP.IsLoopback() {
						// Add IPv6 addresses to the list
						if ipNet.IP.IsGlobalUnicast() {
							if ipNet.IP.To4() == nil && !strings.HasPrefix(ipNet.IP.String(), "fe80") {
								newIPv6 = append(newIPv6, ipNet.IP)
							} else if ipNet.IP.To4() != nil {
								// Add IPv4 addresses to the list
								newIPv4 = append(newIPv4, ipNet.IP)
							}
						}
					}
				}
			}

			// Add the new addresses to the list
			IPv6.Lock()
			IPv6.IPs = newIPv6
			IPv6.Unlock()

			IPv4.Lock()
			IPv4.IPs = newIPv4
			IPv4.Unlock()

			if first {
				c.interfacesWatcherStarted <- true
				close(c.interfacesWatcherStarted)
				first = false
			}

			time.Sleep(time.Second)
		}
	}
}

func getNextIP(availableIPs *availableIPs) net.IP {
	availableIPs.Lock()
	defer availableIPs.Unlock()

	if len(availableIPs.IPs) == 0 {
		return nil
	}

	currentIndex := atomic.AddUint32(&availableIPs.Index, 1) - 1
	ip := availableIPs.IPs[int(currentIndex)%len(availableIPs.IPs)]

	return ip
}

func (d *customDialer) getLocalAddr(network, address string) any {
	var destAddr string

	// Check if the address is already an IP
	if IP := net.ParseIP(address); IP != nil {
		destAddr = address
	} else {
		// If it's not an IP, split the host and port
		var err error
		destAddr, _, err = net.SplitHostPort(address)
		if err != nil {
			return nil
		}
		destAddr = strings.Trim(destAddr, "[]")
	}

	destIP := net.ParseIP(destAddr)
	if destIP == nil {
		ctx, cancel := context.WithTimeout(context.Background(), d.DNSResolutionTimeout)
		defer cancel()

		// Determine which IP versions to look up
		var lookupType string
		switch {
		case !d.disableIPv4 && !d.disableIPv6:
			lookupType = "ip"
		case !d.disableIPv4:
			lookupType = "ip4"
		case !d.disableIPv6:
			lookupType = "ip6"
		default:
			return nil // Both IPv4 and IPv6 are disabled
		}

		IPs, err := net.DefaultResolver.LookupIP(ctx, lookupType, destAddr)
		if err != nil || len(IPs) == 0 {
			return nil
		}

		fmt.Printf("Found %+v\n", IPs)

		destIP = IPs[0] // Use the first resolved IP

		fmt.Printf("Resolved %s to %s\n", destAddr, destIP)

		// Store the resolved IP in d.DNSResolutions
		d.DNSResolutions.Store(destAddr, destIP)
	}

	var localIP net.IP
	if destIP.To4() != nil {
		if d.disableIPv4 {
			return nil
		}
		localIP = getNextIP(IPv4)
	} else {
		if d.disableIPv6 {
			return nil
		}
		localIP = getNextIP(IPv6)
	}

	switch network {
	case "tcp":
		return &net.TCPAddr{IP: localIP}
	case "udp":
		return &net.UDPAddr{IP: localIP}
	default:
		return nil
	}
}
