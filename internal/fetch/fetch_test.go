package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"io"
	"net"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"
)

func TestOnePreservesCompressedBodyAndBoundsDecompressedSize(t *testing.T) {
	feed := []byte(`<rss version="2.0"><channel><title>compressed</title></channel></rss>`)
	var compressed bytes.Buffer
	writer := gzip.NewWriter(&compressed)
	if _, err := writer.Write(feed); err != nil {
		t.Fatal(err)
	}
	if err := writer.Close(); err != nil {
		t.Fatal(err)
	}
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Content-Encoding", "gzip")
		_, _ = w.Write(compressed.Bytes())
	}))
	defer server.Close()

	result, err := One(context.Background(), Request{
		URL: server.URL, Timeout: time.Second, MaxBodyBytes: 1 << 20,
		MaxRedirects: 1, AllowPrivateNetwork: true,
	})
	if err != nil {
		t.Fatal(err)
	}
	if !bytes.Equal(result.Body, compressed.Bytes()) {
		t.Fatal("encoded response body was not preserved")
	}
	if !bytes.Equal(result.DecodedBody, feed) {
		t.Fatal("decoded response did not match feed")
	}
}

func TestOneRejectsCrossOriginRedirectCredentials(t *testing.T) {
	var receivedSecret string
	destination := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		receivedSecret = request.Header.Get("X-Feed-Token")
		_, _ = w.Write([]byte("ok"))
	}))
	defer destination.Close()
	source := httptest.NewServer(http.RedirectHandler(destination.URL, http.StatusFound))
	defer source.Close()

	_, err := One(context.Background(), Request{
		URL: source.URL, Timeout: time.Second, MaxBodyBytes: 1024, MaxRedirects: 2,
		AllowPrivateNetwork: true, Headers: map[string]string{"X-Feed-Token": "secret"},
	})
	if err != nil {
		t.Fatal(err)
	}
	if receivedSecret != "" {
		t.Fatal("sensitive header was forwarded to another origin")
	}
}

func TestHTTPErrorExposesRetryAfter(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		w.Header().Set("Retry-After", "17")
		http.Error(w, "busy", http.StatusTooManyRequests)
	}))
	defer server.Close()
	before := time.Now()
	_, err := One(context.Background(), Request{
		URL: server.URL, Timeout: time.Second, MaxBodyBytes: 1024, MaxRedirects: 1,
		AllowPrivateNetwork: true,
	})
	if err == nil || !IsTransient(err) {
		t.Fatalf("error = %v, want retryable", err)
	}
	retryAt := RetryAt(err)
	if retryAt.Before(before.Add(16*time.Second)) || retryAt.After(before.Add(19*time.Second)) {
		t.Fatalf("retry time = %s", retryAt)
	}
}

func TestOneDefaultsToPublicDestinations(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, "unreachable")
	}))
	defer server.Close()
	_, err := One(context.Background(), Request{
		URL: server.URL, Timeout: time.Second, MaxBodyBytes: 1024,
	})
	if err == nil {
		t.Fatal("private network request unexpectedly accepted")
	}

}

func TestOneHonorsTimeout(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		time.Sleep(50 * time.Millisecond)
		_, _ = w.Write([]byte("late"))
	}))
	defer server.Close()
	_, err := One(context.Background(), Request{
		URL: server.URL, Timeout: 5 * time.Millisecond, MaxBodyBytes: 1024,
		AllowPrivateNetwork: true,
	})
	if err == nil || !IsTransient(err) {
		t.Fatalf("error = %v, want transient timeout", err)
	}
}

func TestOneUsesApprovedProxyForFeedAndRedirectWithoutLocalDNS(t *testing.T) {
	const first = "http://feed.invalid/rss"
	const second = "http://publisher.invalid/feed"
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch req.URL.String() {
		case first:
			if req.Header.Get("Authorization") != "test-token" {
				t.Error("configured feed authorization missing from original request")
			}
			http.Redirect(w, req, second, http.StatusFound)
		case second:
			if req.Header.Get("Authorization") != "" {
				t.Error("authorization followed cross-origin redirect")
			}
			_, _ = fmt.Fprint(w, `<rss version="2.0"><channel><title>Test</title></channel></rss>`)
		default:
			t.Errorf("unexpected proxy request: %s", req.URL)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer proxy.Close()
	t.Setenv("RSSCOLLECTOR_EGRESS_PROXY", proxy.URL)
	t.Setenv("NO_PROXY", "*")

	result, err := One(context.Background(), Request{
		URL: first, Timeout: time.Second, MaxBodyBytes: 1024, MaxRedirects: 1,
		Headers: map[string]string{"Authorization": "test-token"},
	})
	if err != nil || result.Status != http.StatusOK || result.EffectiveURL != second ||
		len(result.Redirects) != 1 || result.Redirects[0] != second {
		t.Fatalf("proxy result = %+v, err = %v", result, err)
	}
}

func TestOneRejectsPrivateRedirectBeforeRoutingToApprovedProxy(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.URL.Hostname() != "publisher.invalid" {
			t.Errorf("unexpected proxy request: %s", req.URL)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		http.Redirect(w, req, "http://127.0.0.1/rss", http.StatusFound)
	}))
	defer proxy.Close()
	t.Setenv("RSSCOLLECTOR_EGRESS_PROXY", proxy.URL)

	_, err := One(context.Background(), Request{
		URL: "http://publisher.invalid/rss", Timeout: time.Second,
		MaxBodyBytes: 1024, MaxRedirects: 1, AllowPrivateNetwork: true,
	})
	if err == nil || !strings.Contains(err.Error(), "not a public address") {
		t.Fatalf("redirect error = %v, want destination rejection", err)
	}
}

func TestOneUsesConnectForHTTPSAndSurfacesProxyRejection(t *testing.T) {
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodConnect || req.Host != "publisher.invalid:443" {
			t.Errorf("proxy request = %s %s, want CONNECT publisher.invalid:443", req.Method, req.Host)
		}
		if req.Header.Get("Authorization") != "" {
			t.Error("feed authorization was sent in CONNECT")
		}
		w.WriteHeader(http.StatusForbidden)
	}))
	defer proxy.Close()
	t.Setenv("RSSCOLLECTOR_EGRESS_PROXY", proxy.URL)

	_, err := One(context.Background(), Request{
		URL: "https://publisher.invalid/rss", Timeout: time.Second, MaxBodyBytes: 1024,
		Headers: map[string]string{"Authorization": "test-token"},
	})
	if err == nil || !strings.Contains(err.Error(), "Forbidden") {
		t.Fatalf("proxy error = %v, want Forbidden", err)
	}
}

func TestOneFetchesHTTPSFeedThroughProxyWithTLSVerification(t *testing.T) {
	const body = `<rss version="2.0"><channel><title>Test</title></channel></rss>`
	origin := httptest.NewTLSServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Host != "example.com" || req.URL.Path != "/feed" {
			t.Errorf("unexpected origin request: %s %s", req.Host, req.URL.Path)
		}
		fmt.Fprint(w, body)
	}))
	defer origin.Close()

	transport := http.DefaultTransport
	http.DefaultTransport = origin.Client().Transport
	t.Cleanup(func() { http.DefaultTransport = transport })
	proxy := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if req.Method != http.MethodConnect ||
			(req.Host != "example.com:443" && req.Host != "wrong.example.net:443") {
			t.Errorf("unexpected proxy request: %s %s", req.Method, req.Host)
			w.WriteHeader(http.StatusBadRequest)
			return
		}
		upstream, err := net.DialTimeout("tcp", origin.Listener.Addr().String(), time.Second)
		if err != nil {
			t.Error(err)
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		defer upstream.Close()
		client, buffer, err := http.NewResponseController(w).Hijack()
		if err != nil {
			t.Error(err)
			return
		}
		defer client.Close()
		deadline := time.Now().Add(3 * time.Second)
		_ = client.SetDeadline(deadline)
		_ = upstream.SetDeadline(deadline)
		if _, err := buffer.WriteString("HTTP/1.1 200 Connection Established\r\n\r\n"); err != nil {
			t.Error(err)
			return
		}
		if err := buffer.Flush(); err != nil {
			t.Error(err)
			return
		}
		forwarded := make(chan struct{})
		go func() {
			defer close(forwarded)
			_, _ = io.Copy(upstream, buffer)
			_ = upstream.Close()
		}()
		_, _ = io.Copy(client, upstream)
		_ = client.Close()
		<-forwarded
	}))
	defer proxy.Close()
	t.Setenv("RSSCOLLECTOR_EGRESS_PROXY", proxy.URL)

	request := Request{URL: "https://example.com/feed", Timeout: 2 * time.Second, MaxBodyBytes: 1024}
	result, err := One(context.Background(), request)
	if err != nil || result.Status != http.StatusOK || string(result.DecodedBody) != body {
		t.Fatalf("HTTPS proxy response = %+v, err = %v", result, err)
	}
	request.URL = "https://wrong.example.net/feed"
	if _, err := One(context.Background(), request); err == nil || !strings.Contains(err.Error(), "certificate") {
		t.Fatalf("wrong hostname returned %v, want certificate error", err)
	}
}

func TestOneRejectsInvalidProxyWithoutLoggingCredentials(t *testing.T) {
	t.Setenv("RSSCOLLECTOR_EGRESS_PROXY", "http://user:secret@proxy.example")
	_, err := One(context.Background(), Request{URL: "https://publisher.invalid/rss"})
	if err == nil || !strings.Contains(err.Error(), "RSSCOLLECTOR_EGRESS_PROXY") || strings.Contains(err.Error(), "secret") {
		t.Fatalf("invalid proxy error should identify the setting without leaking its value: %v", err)
	}
}
