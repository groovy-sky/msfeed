package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
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
