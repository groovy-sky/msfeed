package fetch

import (
	"context"
	"errors"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
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
}

type Result struct {
	Body         []byte
	Status       int
	EffectiveURL string
	ContentType  string
	ETag         string
	LastModified string
	NotModified  bool
	RetrievedAt  time.Time
}

func One(ctx context.Context, request Request) (Result, error) {
	if err := validateDestination(ctx, request.URL, request.AllowPrivateNetwork); err != nil {
		return Result{}, err
	}
	redirects := 0
	client := &http.Client{
		Timeout: request.Timeout,
		CheckRedirect: func(req *http.Request, via []*http.Request) error {
			redirects++
			if redirects > request.MaxRedirects {
				return fmt.Errorf("redirect limit of %d exceeded", request.MaxRedirects)
			}
			return validateDestination(req.Context(), req.URL.String(), request.AllowPrivateNetwork)
		},
	}
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, request.URL, nil)
	if err != nil {
		return Result{}, fmt.Errorf("create request: %w", err)
	}
	req.Header.Set("User-Agent", request.UserAgent)
	req.Header.Set("Accept", "application/rss+xml, application/atom+xml, application/rdf+xml, application/xml, text/xml, */*;q=0.5")
	if request.ETag != "" {
		req.Header.Set("If-None-Match", request.ETag)
	}
	if request.LastModified != "" {
		req.Header.Set("If-Modified-Since", request.LastModified)
	}
	resp, err := client.Do(req)
	if err != nil {
		return Result{}, fmt.Errorf("fetch %s: %w", request.URL, err)
	}
	defer resp.Body.Close()

	now := time.Now()
	result := Result{
		Status: resp.StatusCode, EffectiveURL: resp.Request.URL.String(),
		ContentType: resp.Header.Get("Content-Type"), ETag: resp.Header.Get("ETag"),
		LastModified: resp.Header.Get("Last-Modified"), RetrievedAt: now,
	}
	if resp.StatusCode == http.StatusNotModified {
		result.NotModified = true
		return result, nil
	}
	if resp.StatusCode < 200 || resp.StatusCode >= 300 {
		_, _ = io.Copy(io.Discard, io.LimitReader(resp.Body, 4096))
		return Result{}, fmt.Errorf("fetch %s: unexpected HTTP status %d", request.URL, resp.StatusCode)
	}
	body, err := io.ReadAll(io.LimitReader(resp.Body, request.MaxBodyBytes+1))
	if err != nil {
		return Result{}, fmt.Errorf("read response: %w", err)
	}
	if int64(len(body)) > request.MaxBodyBytes {
		return Result{}, fmt.Errorf("response exceeds limit of %d bytes", request.MaxBodyBytes)
	}
	result.Body = body
	return result, nil
}

func validateDestination(ctx context.Context, raw string, allowPrivate bool) error {
	parsed, err := url.Parse(raw)
	if err != nil || parsed.Hostname() == "" {
		return fmt.Errorf("invalid destination %q", raw)
	}
	if parsed.Scheme != "http" && parsed.Scheme != "https" {
		return errors.New("feed URL must use http or https")
	}
	if parsed.User != nil {
		return errors.New("feed URL must not contain credentials")
	}
	if allowPrivate {
		return nil
	}
	ips, err := net.DefaultResolver.LookupIPAddr(ctx, parsed.Hostname())
	if err != nil {
		return fmt.Errorf("resolve %s: %w", parsed.Hostname(), err)
	}
	for _, address := range ips {
		ip := address.IP
		if ip.IsPrivate() || ip.IsLoopback() || ip.IsLinkLocalUnicast() || ip.IsUnspecified() {
			return fmt.Errorf("destination %s resolves to a non-public address", parsed.Hostname())
		}
	}
	return nil
}

func IsTransient(err error) bool {
	return err != nil && (errors.Is(err, context.DeadlineExceeded) ||
		strings.Contains(err.Error(), "unexpected HTTP status 429") ||
		strings.Contains(err.Error(), "unexpected HTTP status 500") ||
		strings.Contains(err.Error(), "unexpected HTTP status 502") ||
		strings.Contains(err.Error(), "unexpected HTTP status 503") ||
		strings.Contains(err.Error(), "unexpected HTTP status 504"))
}
