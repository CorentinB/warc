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

	t.Logf("Generated IPv6 address: %v", ip)

	if !baseIPNet.Contains(ip) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip, baseIPNet)
	}
}

func TestGenerateRandomIPv6Uniqueness(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.CIDRMask(32, 128),
	}

	// Generate the first IP
	ip1, err := generateRandomIPv6(baseIPNet)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	t.Logf("First generated IPv6 address: %v", ip1)

	if !baseIPNet.Contains(ip1) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip1, baseIPNet)
	}

	// Generate the second IP
	ip2, err := generateRandomIPv6(baseIPNet)
	if err != nil {
		t.Errorf("Expected no error, got %v", err)
	}

	t.Logf("Second generated IPv6 address: %v", ip2)

	if !baseIPNet.Contains(ip2) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip2, baseIPNet)
	}

	// Check if the two IPs are different
	if ip1.Equal(ip2) {
		t.Errorf("Expected different IPs, got the same IP %v", ip1)
	}
}

// TestGenerateRandomIPv6InvalidBaseIP tests the function with an invalid base IP.
func TestGenerateRandomIPv6InvalidBaseIP(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("invalidIP"),
		Mask: net.CIDRMask(64, 128),
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid base IP, got nil")
	} else {
		t.Logf("Received expected error: %v", err)
	}

	if ip != nil {
		t.Logf("Generated IP (unexpectedly): %v", ip)
	}
}

// TestGenerateRandomIPv6InvalidMask tests the function with an invalid mask length.
func TestGenerateRandomIPv6InvalidMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.CIDRMask(129, 128),
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid mask length, got nil")
	} else {
		t.Logf("Received expected error for invalid mask length: %v", err)
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
	ip, err = generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for invalid mask length, got nil")
	} else {
		t.Logf("Received expected error for non-contiguous mask: %v", err)
	}

	if ip != nil {
		t.Logf("Generated IP (unexpectedly): %v", ip)
	}
}

// TestGenerateRandomIPv6EmptyMask tests the function with an empty mask.
func TestGenerateRandomIPv6EmptyMask(t *testing.T) {
	baseIPNet := net.IPNet{
		IP:   net.ParseIP("2001:db8::"),
		Mask: net.IPMask([]byte{}), // Empty mask
	}
	ip, err := generateRandomIPv6(baseIPNet)
	if err == nil {
		t.Error("Expected error for empty mask, got nil")
	} else {
		t.Logf("Received expected error for empty mask: %v", err)
	}

	if ip != nil {
		t.Logf("Generated IP (unexpectedly): %v", ip)
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
	t.Logf("Generated IPv6 address with full mask: %v", ip)

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
	t.Logf("Generated IPv6 address with zero mask: %v", ip)

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
	t.Logf("First IP selected: %v", ip)
	if !ip.Equal(ip1) {
		t.Errorf("Expected %v, got %v", ip1, ip)
	}
	ip = getNextIP(availableIPs)
	t.Logf("Second IP selected: %v", ip)
	if !ip.Equal(ip2) {
		t.Errorf("Expected %v, got %v", ip2, ip)
	}
	ip = getNextIP(availableIPs)
	t.Logf("Third IP selected (should cycle back to first): %v", ip)
	if !ip.Equal(ip1) {
		t.Errorf("Expected %v, got %v", ip1, ip)
	}
}

// TestGetNextIPEmptyIPs tests the function when no IPs are available.
func TestGetNextIPEmptyIPs(t *testing.T) {
	availableIPs := &availableIPs{}
	availableIPs.IPs.Store(&[]net.IPNet{})
	ip := getNextIP(availableIPs)
	t.Logf("Generated IP with empty IP list: %v", ip)
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
	t.Logf("Generated IP: %v", ip)
	if ip == nil {
		t.Errorf("Expected non-nil IP, got nil")
	}
	if !baseIPNet.Contains(ip) {
		t.Errorf("Generated IP %v is not within base IP network %v", ip, baseIPNet)
	}
}

// TestGetNextIPAnyIPMultipleIPv6 tests the function with multiple IPv6 addresses and AnyIP set to true.
func TestGetNextIPAnyIPMultipleIPv6(t *testing.T) {
	baseIP1 := net.ParseIP("2001:db8::")
	baseIPNet1 := net.IPNet{IP: baseIP1, Mask: net.CIDRMask(64, 128)}
	baseIP2 := net.ParseIP("2001:db9::")
	baseIPNet2 := net.IPNet{IP: baseIP2, Mask: net.CIDRMask(64, 128)}
	ipList := []net.IPNet{baseIPNet1, baseIPNet2}
	availableIPs := &availableIPs{AnyIP: true}
	availableIPs.IPs.Store(&ipList)

	for i := 0; i < 5; i++ {
		ip := getNextIP(availableIPs)
		t.Logf("Generated IP #%d: %v", i+1, ip)
		if ip == nil {
			t.Errorf("Expected non-nil IP, got nil")
		}
		if !baseIPNet1.Contains(ip) && !baseIPNet2.Contains(ip) {
			t.Errorf("Generated IP %v is not within base IP network %v or %v", ip, baseIPNet1, baseIPNet2)
		}
	}
}

// TestGetNextIPAnyIPv6Randomness tests the randomness of generated IPv6 addresses.
func TestGetNextIPAnyIPv6Randomness(t *testing.T) {
	baseIP := net.ParseIP("2001:db8::")
	baseIPNet := net.IPNet{IP: baseIP, Mask: net.CIDRMask(64, 128)}
	ipList := []net.IPNet{baseIPNet}
	availableIPs := &availableIPs{AnyIP: true}
	availableIPs.IPs.Store(&ipList)

	ip1 := getNextIP(availableIPs)
	t.Logf("First randomly generated IP: %v", ip1)
	ip2 := getNextIP(availableIPs)
	t.Logf("Second randomly generated IP: %v", ip2)
	if ip1.Equal(ip2) {
		t.Errorf("Expected different IPs, got %v", ip1)
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
	t.Logf("Generated IP with high index: %v", ip)
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

	addr := getLocalAddr("tcp", "192.168.1.2")
	t.Logf("Selected local IPv4 TCP address: %v", addr)
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

	addr := getLocalAddr("tcp6", "[2001:db8::2]")
	t.Logf("Selected local IPv6 TCP address: %v", addr)
	tcpAddr, ok := addr.(*net.TCPAddr)
	if !ok {
		t.Errorf("Expected *net.TCPAddr, got %T", addr)
	}
	if !tcpAddr.IP.Equal(ip1) {
		t.Errorf("Expected IP %v, got %v", ip1, tcpAddr.IP)
	}
}
