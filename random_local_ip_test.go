package warc

import (
	"net"
	"testing"
)

// TestGenerateRandomIPv6 tests the generation of a random IPv6 address within a given subnet.
func TestGenerateRandomIPv6(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.CIDRMask(64, 128),
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !baseIPNet.Contains(ip) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip, baseIPNet)
	}
}

// TestGenerateRandomIPv6InvalidBaseIP tests the function with an invalid base IP.
func TestGenerateRandomIPv6InvalidBaseIP(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("invalidIP"),
		Mask: net.CIDRMask(64, 128),
	}
	_, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid base IP, got nil")
	}
}

// TestGenerateRandomIPv6InvalidMask tests the function with an invalid mask length.
func TestGenerateRandomIPv6InvalidMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.CIDRMask(129, 128),
	}
	_, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid mask length, got nil")
	}

	baseIPNet = net.IPNet{
		IP: net.ParseIP("2001:db8::"),
		Mask: net.IPMask([]byte{
			0xff, 0xfe, 0xff, 0xff, // Non-contiguous mask (invalid) (2nd byte)
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
			0xff, 0xff, 0xff, 0xff,
		}),
	}
	_, err = generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid mask length, got nil")
	}
}

func TestGenerateRandomIPv6EmptyMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.IPMask([]byte{}), // Empty mask
	}
	_, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for empty mask, got nil")
	}
}

// TestGenerateRandomIPv6FullMask tests the function with a /128 mask (no host bits).
func TestGenerateRandomIPv6FullMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::1"),
		Mask: net.CIDRMask(128, 128),
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !ip.Equal(baseIPNet.IP) {
		t.Errorf("Expected IP %v, got %v", baseIPNet.IP, ip)
	}
}

// TestGenerateRandomIPv6ZeroMask tests the function with a /0 mask (all host bits).
func TestGenerateRandomIPv6ZeroMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("::"),
		Mask: net.CIDRMask(0, 128),
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}
	if !baseIPNet.Contains(ip) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip, baseIPNet)
	}
}

// TestGetNextIP tests the round-robin IP selection.
func TestGetNextIP(t *testing.T) {
	ip1 := net.ParseIP("192.168.1.1")
	ip2 := net.ParseIP("192.168.1.2")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(24, 32)}
	ipNet2 := net.IPNet{IP: ip2, Mask: net.CIDRMask(24, 32)}
	ipList := []net.IPNet{ipNet1, ipNet2}
	availableIPs := &availableIPs{}
	availableIPs.IPs.Store(&ipList)

	ip := getNextIP(availableIPs)
	if !ip.Equal(ip1) {
		t.Errorf("Expected %v, got %v", ip1, ip)
	}
	ip = getNextIP(availableIPs)
	if !ip.Equal(ip2) {
		t.Errorf("Expected %v, got %v", ip2, ip)
	}
	ip = getNextIP(availableIPs)
	if !ip.Equal(ip1) {
		t.Errorf("Expected %v, got %v", ip1, ip)
	}
}

// TestGetNextIPEmptyIPs tests the function when no IPs are available.
func TestGetNextIPEmptyIPs(t *testing.T) {
	availableIPs := &availableIPs{}
	availableIPs.IPs.Store(&[]net.IPNet{})
	ip := getNextIP(availableIPs)
	if ip != nil {
		t.Errorf("Expected nil, got %v", ip)
	}
}

// TestGetNextIPAnyIP tests the function with AnyIP set to true for IPv6.
func TestGetNextIPAnyIP(t *testing.T) {
	baseIP := net.ParseIP("2001:db8::")
	baseIPNet := net.IPNet{IP: baseIP, Mask: net.CIDRMask(64, 128)}
	ipList := []net.IPNet{baseIPNet}
	availableIPs := &availableIPs{AnyIP: true}
	availableIPs.IPs.Store(&ipList)

	ip := getNextIP(availableIPs)
	if ip == nil {
		t.Error("Expected non-nil IP, got nil")
	}
	if !baseIPNet.Contains(ip) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip, baseIPNet)
	}
}

// TestGetNextIPHighIndex tests the function with a high index value.
func TestGetNextIPHighIndex(t *testing.T) {
	ip1 := net.ParseIP("192.168.1.1")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(24, 32)}
	ipList := []net.IPNet{ipNet1}
	availableIPs := &availableIPs{}
	availableIPs.IPs.Store(&ipList)
	availableIPs.Index.Store(9999999)

	ip := getNextIP(availableIPs)
	if !ip.Equal(ip1) {
		t.Errorf("Expected %v, got %v", ip1, ip)
	}
}

// TestGetLocalAddrIPv4TCP tests local address selection for IPv4 TCP connections.
func TestGetLocalAddrIPv4TCP(t *testing.T) {
	IPv4 = &availableIPs{}
	ip1 := net.ParseIP("192.168.1.1")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(24, 32)}
	ipList := []net.IPNet{ipNet1}
	IPv4.IPs.Store(&ipList)

	addr := getLocalAddr("tcp", "192.168.1.2:80")
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Errorf("Expected *net.TCPAddr, got %T", addr)
	}
	if !tcpAddr.IP.Equal(ip1) {
		t.Errorf("Expected IP %v, got %v", ip1, tcpAddr.IP)
	}
}

// TestGetLocalAddrIPv6TCP tests local address selection for IPv6 TCP connections.
func TestGetLocalAddrIPv6TCP(t *testing.T) {
	IPv6 = &availableIPs{}
	ip1 := net.ParseIP("2001:db8::1")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(64, 128)}
	ipList := []net.IPNet{ipNet1}
	IPv6.IPs.Store(&ipList)

	addr := getLocalAddr("tcp", "[2001:db8::2]:80")
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Errorf("Expected *net.TCPAddr, got %T", addr)
	}
	if !tcpAddr.IP.Equal(ip1) {
		t.Errorf("Expected IP %v, got %v", ip1, tcpAddr.IP)
	}
}

// TestGetLocalAddrIPv4UDP tests local address selection for IPv4 UDP connections.
func TestGetLocalAddrIPv4UDP(t *testing.T) {
	IPv4 = &availableIPs{}
	ip1 := net.ParseIP("192.168.1.1")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(24, 32)}
	ipList := []net.IPNet{ipNet1}
	IPv4.IPs.Store(&ipList)

	addr := getLocalAddr("udp", "192.168.1.2:80")
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		t.Errorf("Expected *net.UDPAddr, got %T", addr)
	}
	if !udpAddr.IP.Equal(ip1) {
		t.Errorf("Expected IP %v, got %v", ip1, udpAddr.IP)
	}
}

// TestGetLocalAddrIPv6UDP tests local address selection for IPv6 UDP connections.
func TestGetLocalAddrIPv6UDP(t *testing.T) {
	IPv6 = &availableIPs{}
	ip1 := net.ParseIP("2001:db8::1")
	ipNet1 := net.IPNet{IP: ip1, Mask: net.CIDRMask(64, 128)}
	ipList := []net.IPNet{ipNet1}
	IPv6.IPs.Store(&ipList)

	addr := getLocalAddr("udp", "[2001:db8::2]:80")
	udpAddr, ok := addr.(*net.UDPAddr)
	if !ok {
		t.Errorf("Expected *net.UDPAddr, got %T", addr)
	}
	if !udpAddr.IP.Equal(ip1) {
		t.Errorf("Expected IP %v, got %v", ip1, udpAddr.IP)
	}
}

// TestGetLocalAddrInvalidIP tests the function with an invalid destination IP.
func TestGetLocalAddrInvalidIP(t *testing.T) {
	addr := getLocalAddr("tcp", "invalidIP:80")
	if addr != nil {
		t.Errorf("Expected nil, got %v", addr)
	}
}

// TestGetLocalAddrUnknownNetwork tests the function with an unknown network type.
func TestGetLocalAddrUnknownNetwork(t *testing.T) {
	addr := getLocalAddr("unknown", "192.168.1.2:80")
	if addr != nil {
		t.Errorf("Expected nil, got %v", addr)
	}
}

// TestGetLocalAddrNoPort tests the function with an address missing a port.
func TestGetLocalAddrNoPort(t *testing.T) {
	addr := getLocalAddr("tcp", "192.168.1.2")
	if addr != nil {
		t.Errorf("Expected nil, got %v", addr)
	}
}

// TestGetLocalAddrMalformedAddress tests the function with a malformed address.
func TestGetLocalAddrMalformedAddress(t *testing.T) {
	addr := getLocalAddr("tcp", "192.168.1.2::80")
	if addr != nil {
		t.Errorf("Expected nil, got %v", addr)
	}
}

// TestGetAvailableIPs is difficult due to its infinite loop and dependency on system interfaces.
// It's recommended to refactor the function for better testability or to use integration tests.
