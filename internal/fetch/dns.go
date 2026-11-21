package fetch

import (
	"context"
	"errors"
	"fmt"
	"net"
	"net/netip"
	"strings"
	"time"
)

var publicDNSServers = [...]string{
	"1.1.1.1", "1.0.0.1", "9.9.9.9", "149.112.112.112",
	"8.8.8.8", "8.8.4.4", "208.67.222.222", "208.67.220.220",
}

var sharedAddressSpace = &net.IPNet{IP: net.IPv4(100, 64, 0, 0).To4(), Mask: net.CIDRMask(10, 32)}

type ipLookup func(context.Context, string) ([]net.IPAddr, error)

func lookupWithFallback(ctx context.Context, host string) ([]net.IPAddr, error) {
	return resolveIP(ctx, host, net.DefaultResolver.LookupIPAddr, publicDNSServers[:], func(ctx context.Context, server, name string) ([]net.IPAddr, error) {
		return lookupAt(ctx, net.JoinHostPort(server, "53"), name)
	})
}

func resolveIP(ctx context.Context, host string, system ipLookup, servers []string,
	query func(context.Context, string, string) ([]net.IPAddr, error),
) ([]net.IPAddr, error) {
	if ip, err := netip.ParseAddr(host); err == nil {
		return []net.IPAddr{{IP: ip.AsSlice(), Zone: ip.Zone()}}, nil
	}
	systemCtx, cancel := context.WithTimeout(ctx, 3*time.Second)
	ips, systemErr := system(systemCtx, host)
	cancel()
	if systemErr == nil && len(ips) > 0 {
		return ips, nil
	}
	if systemErr == nil {
		systemErr = fmt.Errorf("no addresses")
	}
	name := strings.ToLower(strings.TrimSuffix(host, "."))
	if !strings.Contains(name, ".") || strings.HasSuffix(name, ".localhost") || strings.HasSuffix(name, ".local") {
		return nil, systemErr
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}

	var lastErr error
	for _, server := range servers {
		attempt, cancel := context.WithTimeout(ctx, time.Second)
		ips, lastErr = query(attempt, server, name+".")
		cancel()
		if lastErr == nil && len(ips) > 0 {
			return ips, nil
		}
		if err := ctx.Err(); err != nil {
			return nil, err
		}
		if lastErr == nil {
			lastErr = fmt.Errorf("no addresses")
		}
	}
	if lastErr == nil {
		return nil, systemErr
	}
	return nil, fmt.Errorf("system DNS: %v; public DNS fallback (%d resolvers): %w", systemErr, len(servers), lastErr)
}

func lookupAt(ctx context.Context, server, name string) ([]net.IPAddr, error) {
	dialer := &net.Dialer{}
	resolver := &net.Resolver{
		PreferGo: true,
		Dial: func(ctx context.Context, network, _ string) (net.Conn, error) {
			return dialer.DialContext(ctx, network, server)
		},
	}
	ips, err := resolver.LookupIPAddr(ctx, name)
	if err != nil {
		// net.Resolver can label errors with a system nameserver even when Dial overrides it.
		var dnsErr *net.DNSError
		if errors.As(err, &dnsErr) {
			actual := *dnsErr
			actual.Server = server
			return nil, &actual
		}
		return nil, fmt.Errorf("query DNS resolver %s: %w", server, err)
	}
	return ips, nil
}

type resolvedTargetKey struct{}

type resolvedTarget struct {
	address  string
	ips      []net.IPAddr
	deadline time.Time
}

func dialResolved(dial func(context.Context, string, string) (net.Conn, error)) func(context.Context, string, string) (net.Conn, error) {
	if dial == nil {
		dial = (&net.Dialer{}).DialContext
	}
	return func(ctx context.Context, network, address string) (net.Conn, error) {
		target, ok := ctx.Value(resolvedTargetKey{}).(resolvedTarget)
		if !ok || target.address == "" {
			return dial(ctx, network, address)
		}
		if !strings.EqualFold(target.address, address) {
			return nil, errors.New("dial target does not match resolved feed address")
		}
		_, port, err := net.SplitHostPort(address)
		if err != nil {
			return nil, err
		}
		// Keep the URL hostname for Host and TLS; dial only addresses validated for this hop.
		if !target.deadline.IsZero() {
			var cancel context.CancelFunc
			ctx, cancel = context.WithDeadline(ctx, target.deadline)
			defer cancel()
		}
		var firstErr error
		for i, ip := range target.ips {
			attempt := ctx
			cancel := func() {}
			if deadline, ok := ctx.Deadline(); ok {
				attempt, cancel = context.WithTimeout(ctx, time.Until(deadline)/time.Duration(len(target.ips)-i))
			}
			conn, err := dial(attempt, network, net.JoinHostPort(ip.String(), port))
			cancel()
			if err == nil {
				return conn, nil
			}
			if firstErr == nil {
				firstErr = err
			}
			if ctx.Err() != nil {
				return nil, ctx.Err()
			}
		}
		return nil, firstErr
	}
}
