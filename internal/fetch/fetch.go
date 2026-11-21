package fetch

import (
	"bytes"
	"compress/gzip"
	"compress/zlib"
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strconv"
	"strings"
	"time"

	"golang.org/x/net/idna"
)

type Request struct {
	URL                 string
	ETag                string
	LastModified        string
	UserAgent           string
	Timeout             time.Duration
	MaxBodyBytes        int64
	MaxRedirects        int
	AllowPrivateNetwork bool
	PublicDNSFallback   bool
	Headers             map[string]string
	Now                 func() time.Time
}

type Result struct {
	Body            []byte
	DecodedBody     []byte
	Status          int
	EffectiveURL    string
	Redirects       []string
	ContentType     string
	ContentEncoding string
	ETag            string
	LastModified    string
	RetryAfter      time.Time
	NotModified     bool
	RetrievedAt     time.Time
}

type HTTPError struct {
	URL        string
	StatusCode int
	RetryAfter time.Time
}

func (e *HTTPError) Error() string {
	return fmt.Sprintf("fetch %s: unexpected HTTP status %d", e.URL, e.StatusCode)
}

func (e *HTTPError) Retryable() bool {
	switch e.StatusCode {
	case http.StatusRequestTimeout, http.StatusTooManyRequests,
		http.StatusInternalServerError, http.StatusBadGateway,
		http.StatusServiceUnavailable, http.StatusGatewayTimeout:
		return true
	default:
		return false
	}
}

func One(ctx context.Context, request Request) (Result, error) {
	return one(ctx, request, lookupWithFallback)
}

func one(ctx context.Context, request Request, lookup ipLookup) (Result, error) {
	if request.Timeout > 0 {
		var cancel context.CancelFunc
		ctx, cancel = context.WithTimeout(ctx, request.Timeout)
		defer cancel()
	}
	redirects := 0
	redirectChain := make([]string, 0, request.MaxRedirects)
	defaultTransport, ok := http.DefaultTransport.(*http.Transport)
	if !ok {
		return Result{}, errors.New("http.DefaultTransport is not an *http.Transport")
	}
	transport := defaultTransport.Clone()
	defer transport.CloseIdleConnections()
	transport.DisableCompression = true
	if request.PublicDNSFallback {
		transport.DialContext = dialResolved(transport.DialContext)
	}
	prepare := func(req *http.Request) error {
		return prepareDestination(req, transport, request, lookup)
	}
	client := &http.Client{
		Transport: transport,
		Timeout:   request.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			redirects++
			if redirects > request.MaxRedirects {
				return fmt.Errorf("redirect limit of %d exceeded", request.MaxRedirects)
			}
			if err := prepare(req); err != nil {
				return err
			}
			if len(via) > 0 && !sameOrigin(via[0].URL, req.URL) {
				for header := range request.Headers {
					req.Header.Del(header)
				}
			}
			redirectChain = append(redirectChain, req.URL.String())
			return nil
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
	if err != nil {
		return Result{}, fmt.Errorf("create request: %w", err)
	}
	if err := prepare(req); err != nil {
		return Result{}, err
	}
	req.Header.Set("User-Agent", request.UserAgent)
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/rdf+xml, application/xml, text/xml, */*;q=0.5")
	req.Header.Set("Accept-Encoding", "gzip, deflate")
	for name, value := range request.Headers {
		req.Header.Set(name, value)
	}
	if request.ETag != "" {
		req.Header.Set("If-None-Match", request.ETag)
	}
	if request.LastModified != "" {
		req.Header.Set("If-Modified-Since", request.LastModified)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{Redirects: redirectChain}, fmt.Errorf("fetch %s: %w", request.URL, err)
	}
	defer resp.Body.Close()

	now := time.Now()
	if request.Now != nil {
		now = request.Now()
	}
	result := Result{
		Status: resp.StatusCode, EffectiveURL: resp.Request.URL.String(),
		Redirects: redirectChain, ContentType: resp.Header.Get("Content-Type"),
		ContentEncoding: resp.Header.Get("Content-Encoding"), ETag: resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"), RetrievedAt: now,
		RetryAfter: parseRetryAfter(resp.Header.Get("Retry-After"), now),
	}
	if resp.StatusCode == http.StatusNotModified {
		result.NotModified = true
		return result, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return result, &HTTPError{URL: request.URL, StatusCode: resp.StatusCode, RetryAfter: result.RetryAfter}
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, request.MaxBodyBytes+1))
	if err != nil {
		return result, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > request.MaxBodyBytes {
		return result, fmt.Errorf("response exceeds limit of %d bytes", request.MaxBodyBytes)
	}
	result.Body = body
	decoded, err := DecodeBody(body, result.ContentEncoding, request.MaxBodyBytes)
	if err != nil {
		return result, err
	}
	result.DecodedBody = decoded
	return result, nil
}

func prepareDestination(req *http.Request, transport *http.Transport, request Request, lookup ipLookup) error {
	parsed := req.URL
	host := parsed.Hostname()
	if host == "" {
		return errors.New("feed URL must have a hostname")
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("feed URL must use http or https")
	}
	if parsed.User != nil {
		return errors.New("feed URL must not contain credentials")
	}
	directFallback := request.PublicDNSFallback
	if directFallback && transport.Proxy != nil {
		proxy, err := transport.Proxy(req)
		if err != nil {
			return err
		}
		directFallback = proxy == nil
	}
	if request.PublicDNSFallback && !directFallback {
		*req = *req.WithContext(context.WithValue(req.Context(), resolvedTargetKey{}, resolvedTarget{}))
	}
	if request.AllowPrivateNetwork && !directFallback {
		return nil
	}
	if !directFallback {
		lookup = net.DefaultResolver.LookupIPAddr
	}
	if directFallback && net.ParseIP(host) == nil && !strings.Contains(host, ":") {
		var err error
		host, err = idna.Lookup.ToASCII(host)
		if err != nil {
			return fmt.Errorf("invalid feed hostname: %w", err)
		}
	}
	ips, err := lookup(req.Context(), host)
	if err != nil {
		return fmt.Errorf("resolve %s: %w", host, err)
	}
	if len(ips) == 0 {
		return fmt.Errorf("resolve %s: no addresses", host)
	}
	if !request.AllowPrivateNetwork {
		for _, address := range ips {
			ip := address.IP
			if !ip.IsGlobalUnicast() || ip.IsPrivate() || sharedAddressSpace.Contains(ip) {
				return fmt.Errorf("destination %s resolves to a non-public address", host)
			}
		}
	}
	if directFallback {
		port := parsed.Port()
		if port == "" {
			port = "80"
			if parsed.Scheme == "https" {
				port = "443"
			}
		}
		deadline, _ := req.Context().Deadline()
		target := resolvedTarget{address: net.JoinHostPort(host, port), ips: ips, deadline: deadline}
		*req = *req.WithContext(context.WithValue(req.Context(), resolvedTargetKey{}, target))
	}
	return nil
}

func IsTransient(err error) bool {
	if err == nil {
		return false
	}
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.Retryable()
	}
	var netErr net.Error
	return errors.Is(err, context.DeadlineExceeded) || errors.As(err, &netErr)
}

func RetryAt(err error) time.Time {
	var httpErr *HTTPError
	if errors.As(err, &httpErr) {
		return httpErr.RetryAfter
	}
	return time.Time{}
}

func DecodeBody(body []byte, encoding string, maxBytes int64) ([]byte, error) {
	encoding = strings.ToLower(strings.TrimSpace(encoding))
	if encoding == "" || encoding == "identity" {
		return append([]byte(nil), body...), nil
	}
	var reader io.ReadCloser
	var err error
	switch encoding {
	case "gzip":
		reader, err = gzip.NewReader(bytes.NewReader(body))
	case "deflate":
		reader, err = zlib.NewReader(bytes.NewReader(body))
	default:
		return nil, fmt.Errorf("unsupported Content-Encoding %q", encoding)
	}
	if err != nil {
		return nil, fmt.Errorf("decompress %s response: %w", encoding, err)
	}
	defer reader.Close()
	decoded, err := io.ReadAll(io.LimitReader(reader, maxBytes+1))
	if err != nil {
		return nil, fmt.Errorf("decompress %s response: %w", encoding, err)
	}
	if int64(len(decoded)) > maxBytes {
		return nil, fmt.Errorf("decompressed response exceeds limit of %d bytes", maxBytes)
	}
	return decoded, nil
}

func parseRetryAfter(raw string, now time.Time) time.Time {
	raw = strings.TrimSpace(raw)
	if raw == "" {
		return time.Time{}
	}
	if seconds, err := strconv.ParseInt(raw, 10, 64); err == nil {
		if seconds >= 0 {
			const maximum = int64((365 * 24 * time.Hour) / time.Second)
			if seconds > maximum {
				seconds = maximum
			}
			return now.Add(time.Duration(seconds) * time.Second)
		}
	}
	if value, err := http.ParseTime(raw); err == nil {
		return value
	}
	return time.Time{}
}

func sameOrigin(left, right *url.URL) bool {
	return strings.EqualFold(left.Scheme, right.Scheme) &&
		strings.EqualFold(left.Host, right.Host)
}
