package util

import (
	"errors"
	"fmt"
	"net"
	"strconv"
	"strings"
)

// Parse a host/port string with a default port to use
// if none (or 0 or -1) is specified in `hostPort` string.
func parseHostPort(hostPort string, defaultPort int) (host string, port int, err error) {
	if hostPort != "" {
		host, sPort, err := net.SplitHostPort(hostPort)
		if ae, ok := err.(*net.AddrError); ok && strings.Contains(ae.Err, "missing port") {
			// try appending the current port
			host, sPort, err = net.SplitHostPort(fmt.Sprintf("%s:%d", hostPort, defaultPort))
		}
		if err != nil {
			return "", -1, err
		}
		port, err = strconv.Atoi(strings.TrimSpace(sPort))
		if err != nil {
			return "", -1, err
		}
		if port == 0 || port == -1 {
			port = defaultPort
		}
		return strings.TrimSpace(host), port, nil
	}
	return "", -1, errors.New("no hostport specified")
}

// Generic version that will return an array of URLs based on the given
// advertise, host and port values.
func GetConnectURLs(advertise, host string, port int) ([]string, error) {
	urls := make([]string, 0, 1)

	// short circuit if advertise is set
	if advertise != "" {
		h, p, err := parseHostPort(advertise, port)
		if err != nil {
			return nil, err
		}
		urls = append(urls, net.JoinHostPort(h, strconv.Itoa(p)))
	} else {
		sPort := strconv.Itoa(port)
		_, ips, err := getNonLocalIPsIfHostIsIPAny(host, true)
		for _, ip := range ips {
			urls = append(urls, net.JoinHostPort(ip, sPort))
		}
		if err != nil || len(urls) == 0 {
			// We are here if s.opts.Host is not "0.0.0.0" nor "::", or if for some
			// reason we could not add any URL in the loop above.
			// We had a case where a Windows VM was hosed and would have err == nil
			// and not add any address in the array in the loop above, and we
			// ended-up returning 0.0.0.0, which is problematic for Windows clients.
			// Check for 0.0.0.0 or :: specifically, and ignore if that's the case.
			if host == "0.0.0.0" || host == "::" {
				//todo
				//s.Errorf("Address %q can not be resolved properly", host)
			} else {
				urls = append(urls, net.JoinHostPort(host, sPort))
			}
		}
	}
	return urls, nil
}

// Returns an array of non local IPs if the provided host is
// 0.0.0.0 or ::. It returns the first resolved if `all` is
// false.
// The boolean indicate if the provided host was 0.0.0.0 (or ::)
// so that if the returned array is empty caller can decide
// what to do next.
func getNonLocalIPsIfHostIsIPAny(host string, all bool) (bool, []string, error) {
	ip := net.ParseIP(host)
	// If this is not an IP, we are done
	if ip == nil {
		return false, nil, nil
	}
	// If this is not 0.0.0.0 or :: we have nothing to do.
	if !ip.IsUnspecified() {
		return false, nil, nil
	}

	// todo
	//s.Debugf("Get non local IPs for %q", host)
	var ips []string
	ifaces, _ := net.Interfaces()
	for _, i := range ifaces {
		addrs, _ := i.Addrs()
		for _, addr := range addrs {
			switch v := addr.(type) {
			case *net.IPNet:
				ip = v.IP
			case *net.IPAddr:
				ip = v.IP
			}
			ipStr := ip.String()
			// Skip non global unicast addresses
			if !ip.IsGlobalUnicast() || ip.IsUnspecified() {
				ip = nil
				continue
			}
			//todo
			//s.Debugf("  ip=%s", ipStr)
			ips = append(ips, ipStr)
			if !all {
				break
			}
		}
	}
	return true, ips, nil
}

// AddUrl adds urlStr to the given map. If the string was already present, simply
// bumps the reference count.
// Returns true only if it was added for the first time.
func (m RefCountedUrlSet) AddUrl(urlStr string) bool {
	m[urlStr]++
	return m[urlStr] == 1
}

// RemoveUrl removes urlStr from the given map. If the string is not present, nothing
// is done and false is returned.
// If the string was present, its reference count is decreased. Returns true
// if this was the last reference, false otherwise.
func (m RefCountedUrlSet) RemoveUrl(urlStr string) bool {
	removed := false
	if ref, ok := m[urlStr]; ok {
		if ref == 1 {
			removed = true
			delete(m, urlStr)
		} else {
			m[urlStr]--
		}
	}
	return removed
}

// GetAsStringSlice returns the unique URLs in this map as a slice
func (m RefCountedUrlSet) GetAsStringSlice() []string {
	a := make([]string, 0, len(m))
	for u := range m {
		a = append(a, u)
	}
	return a
}

// This map is used to store URLs string as the key with a reference count as
// the value. This is used to handle gossiped URLs such as connect_urls, etc..
type RefCountedUrlSet map[string]int
