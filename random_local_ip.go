package warc

import (
	"crypto/rand"
	"fmt"
	"math/big"
	"net"
	"strings"
	"sync/atomic"
	"time"
)

var (
	IPv6 *availableIPs
	IPv4 *availableIPs
)

type availableIPs struct {
	IPs   atomic.Pointer[[]net.IPNet]
	Index atomic.Uint64
	AnyIP bool
}

func getAvailableIPs(IPv6AnyIP bool) (IPs []net.IP, err error) {
	if IPv6 == nil {
		IPv6 = &availableIPs{
			AnyIP: IPv6AnyIP,
		}
	}

	if IPv4 == nil {
		IPv4 = &availableIPs{}
	}

	for {
		// Get all network interfaces
		interfaces, err := net.Interfaces()
		if err != nil {
			time.Sleep(time.Second)
			continue
		}

		// Iterate over the interfaces
		newIPv4 := make([]net.IPNet, 0)
		newIPv6 := make([]net.IPNet, 0)
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
				if ipNet, ok := addr.(*net.IPNet); ok {
					ip := ipNet.IP

					if ip.IsLoopback() {
						continue
					}

					// Process Global Unicast IPv6 addresses
					if ip.IsGlobalUnicast() && ip.To16() != nil && ip.To4() == nil && ip.IsGlobalUnicast() {
						newIPv6 = append(newIPv6, *ipNet)
					}

					// Process Global Unicast IPv4 addresses
					if ip.IsGlobalUnicast() && ip.To16() == nil && ip.To4() != nil {
						// Add IPv4 addresses to the list
						newIPv4 = append(newIPv4, *ipNet)
					}
				}
			}
		}

		// Add the new addresses to the list
		IPv6.IPs.Store(&newIPv6)
		IPv4.IPs.Store(&newIPv4)

		time.Sleep(time.Second)
	}
}

func getNextIP(availableIPs *availableIPs) net.IP {
	IPsPtr := availableIPs.IPs.Load()
	if IPsPtr == nil {
		return nil
	}

	IPs := *IPsPtr
	if len(IPs) == 0 {
		return nil
	}

	currentIndex := availableIPs.Index.Add(1) - 1
	ipNet := IPs[currentIndex%uint64(len(IPs))]

	if availableIPs.AnyIP && ipNet.IP.To4() == nil && ipNet.IP.To16() != nil {
		ip, err := generateRandomIPv6(ipNet)
		if err == nil {
			return ip
		}
	}

	return ipNet.IP
}

func getLocalAddr(network, address string) any {
	lastColon := strings.LastIndex(address, ":")
	destAddr := address[:lastColon]

	destAddr = strings.TrimPrefix(destAddr, "[")
	destAddr = strings.TrimSuffix(destAddr, "]")

	destIP := net.ParseIP(destAddr)
	if destIP == nil {
		return nil
	}

	if destIP.To4() != nil {
		if network == "tcp" {
			return &net.TCPAddr{IP: getNextIP(IPv4)}
		} else if network == "udp" {
			return &net.UDPAddr{IP: getNextIP(IPv4)}
		}
		return nil
	} else {
		if network == "tcp" {
			return &net.TCPAddr{IP: getNextIP(IPv6)}
		} else if network == "udp" {
			return &net.UDPAddr{IP: getNextIP(IPv6)}
		}
		return nil
	}
}

func generateRandomIPv6(baseIPv6Net net.IPNet) (net.IP, error) {
	baseIP := baseIPv6Net.IP.To16()
	if baseIP == nil {
		return nil, fmt.Errorf("invalid base IPv6 address")
	}

	maskLength, _ := baseIPv6Net.Mask.Size()
	if maskLength < 0 || maskLength > 128 {
		return nil, fmt.Errorf("invalid network mask length")
	}

	hostBits := 128 - maskLength

	max := new(big.Int).Lsh(big.NewInt(1), uint(hostBits)) // 2^hostBits
	randomBits, err := rand.Int(rand.Reader, max)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random bits: %v", err)
	}

	randomizedIP := make(net.IP, len(baseIP))
	copy(randomizedIP, baseIP)

	for i := maskLength / 8; i < 16; i++ {
		remainingBits := 8 * (15 - i) // Shift bits to the correct byte position
		randomizedIP[i] = baseIP[i] | byte(randomBits.Int64()>>remainingBits)
	}

	return randomizedIP, nil
}
