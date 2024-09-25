package warc

import (
	"crypto/rand"
	"fmt"
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
					if ip.IsGlobalUnicast() && ip.To16() != nil && ip.To4() == nil {
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

func getLocalAddr(destNetwork, destAddress string) any {
	lastColon := strings.LastIndex(destAddress, ":")
	if lastColon == -1 {
		return nil
	}
	destAddr := destAddress[:lastColon]

	destAddr = strings.TrimPrefix(destAddr, "[")
	destAddr = strings.TrimSuffix(destAddr, "]")

	destIP := net.ParseIP(destAddr)
	if destIP == nil {
		return nil
	}

	if destIP.To4() != nil {
		if strings.Contains(destNetwork, "tcp") {
			return &net.TCPAddr{IP: getNextIP(IPv4)}
		} else if strings.Contains(destNetwork, "udp") {
			return &net.UDPAddr{IP: getNextIP(IPv4)}
		}
		return nil
	} else {
		if strings.Contains(destNetwork, "tcp") {
			return &net.TCPAddr{IP: getNextIP(IPv6)}
		} else if strings.Contains(destNetwork, "udp") {
			return &net.UDPAddr{IP: getNextIP(IPv6)}
		}
		return nil
	}
}

func generateRandomIPv6(baseIPv6Net net.IPNet) (net.IP, error) {
	baseIP := baseIPv6Net.IP.To16()
	if baseIP == nil || len(baseIPv6Net.Mask) != net.IPv6len {
		return nil, fmt.Errorf("invalid base IPv6 address or mask")
	}

	ones, bits := baseIPv6Net.Mask.Size()
	if bits != 128 || ones < 0 || ones > bits {
		return nil, fmt.Errorf("invalid network mask length")
	}

	hostBits := bits - ones

	// Generate random host bits
	nBytes := (hostBits + 7) / 8 // Number of bytes needed for host bits
	randomBytes := make([]byte, nBytes)
	_, err := rand.Read(randomBytes)
	if err != nil {
		return nil, fmt.Errorf("failed to generate random bits: %v", err)
	}

	// Mask the random bytes if hostBits is not a multiple of 8
	if hostBits%8 != 0 {
		extraBits := 8 - (hostBits % 8)
		randomBytes[0] = randomBytes[0] & (0xFF >> extraBits)
	}

	// Construct the randomized IP address
	randomizedIP := make(net.IP, net.IPv6len)
	copy(randomizedIP, baseIP.Mask(baseIPv6Net.Mask)) // Copy the network prefix

	// Apply the random host bits to the randomized IP
	for i := 0; i < nBytes; i++ {
		randomizedIP[16-nBytes+i] |= randomBytes[i]
	}

	return randomizedIP, nil
}
