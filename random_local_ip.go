package warc

import (
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

func getLocalAddr(network, IP string) any {
	destIP := net.ParseIP(strings.Trim(IP, "[]"))
	if destIP == nil {
		return nil
	}

	if destIP.To4() != nil {
		if strings.Contains(network, "tcp") {
			return &net.TCPAddr{IP: getNextIP(IPv4)}
		} else if strings.Contains(network, "udp") {
			return &net.UDPAddr{IP: getNextIP(IPv4)}
		}
		return nil
	} else {
		if strings.Contains(network, "tcp") {
			return &net.TCPAddr{IP: getNextIP(IPv6)}
		} else if strings.Contains(network, "udp") {
			return &net.UDPAddr{IP: getNextIP(IPv6)}
		}
		return nil
	}
}
