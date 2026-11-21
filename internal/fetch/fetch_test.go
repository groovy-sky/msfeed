package fetch

import (
	"bytes"
	"compress/gzip"
	"context"
	"errors"
	"fmt"
	"net"
	"net/http"
	"net/http/httptest"
	"reflect"
	"strings"
	"testing"
	"time"

	"golang.org/x/net/dns/dnsmessage"
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

func TestResolveIPUsesSystemFirstThenFallbackInOrder(t *testing.T) {
	answer := []net.IPAddr{{IP: net.ParseIP("93.184.216.34")}}
	for _, tc := range []struct {
		name         string
		systemResult []net.IPAddr
		want         []string
	}{
		{"system succeeds", answer, nil},
		{"system fails", nil, []string{"first", "second"}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			var tried []string
			got, err := resolveIP(context.Background(), "Publisher.Invalid",
				func(context.Context, string) ([]net.IPAddr, error) {
					if tc.systemResult == nil {
						return nil, errors.New("system lookup failed")
					}
					return tc.systemResult, nil
				}, []string{"first", "second", "third"}, func(ctx context.Context, server, name string) ([]net.IPAddr, error) {
					if name != "publisher.invalid." {
						t.Errorf("fallback DNS name = %q", name)
					}
					if deadline, ok := ctx.Deadline(); !ok || time.Until(deadline) > time.Second {
						t.Error("fallback resolver has no per-attempt timeout")
					}
					tried = append(tried, server)
					if server == "first" {
						return nil, errors.New("first resolver failed")
					}
					return answer, nil
				})
			if err != nil || !reflect.DeepEqual(answer, got) || !reflect.DeepEqual(tc.want, tried) {
				t.Fatalf("addresses = %v, err = %v, resolvers = %v", got, err, tried)
			}
		})
	}
}

func TestOneDialsDNSAnswerAndPreservesRedirectHost(t *testing.T) {
	dns, err := net.ListenPacket("udp4", "127.0.0.1:0")
	if err != nil {
		t.Fatal(err)
	}
	defer dns.Close()
	go func() {
		packet := make([]byte, 4096)
		for {
			n, peer, err := dns.ReadFrom(packet)
			if err != nil {
				return
			}
			var msg dnsmessage.Message
			if err := msg.Unpack(packet[:n]); err != nil || len(msg.Questions) != 1 {
				continue
			}
			q := msg.Questions[0]
			msg.Response = true
			if q.Type == dnsmessage.TypeA {
				msg.Answers = []dnsmessage.Resource{{
					Header: dnsmessage.ResourceHeader{Name: q.Name, Class: dnsmessage.ClassINET, TTL: 30},
					Body:   &dnsmessage.AResource{A: [4]byte{127, 0, 0, 1}},
				}}
			}
			if response, err := msg.Pack(); err == nil {
				_, _ = dns.WriteTo(response, peer)
			}
		}
	}()

	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasPrefix(req.Host, "start.invalid:"):
			http.Redirect(w, req, "http://"+strings.Replace(req.Host, "start", "end", 1)+"/rss", http.StatusFound)
		case strings.HasPrefix(req.Host, "end.invalid:") && req.URL.Path == "/rss":
			_, _ = fmt.Fprint(w, "feed")
		default:
			t.Errorf("unexpected origin request: %s %s", req.Host, req.URL.Path)
			w.WriteHeader(http.StatusBadRequest)
		}
	}))
	defer server.Close()
	t.Setenv("NO_PROXY", "*")
	var lookedUp []string
	lookup := func(ctx context.Context, host string) ([]net.IPAddr, error) {
		lookedUp = append(lookedUp, host)
		return resolveIP(ctx, host, func(context.Context, string) ([]net.IPAddr, error) {
			return nil, errors.New("system lookup failed")
		}, []string{dns.LocalAddr().String()}, lookupAt)
	}
	req := Request{
		URL:     strings.Replace(server.URL, "127.0.0.1", "start.invalid", 1),
		Timeout: 3 * time.Second, MaxBodyBytes: 1024, MaxRedirects: 1, PublicDNSFallback: true,
	}
	if _, err := one(context.Background(), req, lookup); err == nil || !strings.Contains(err.Error(), "non-public address") {
		t.Fatalf("private fallback answer: %v", err)
	}
	lookedUp = nil
	req.AllowPrivateNetwork = true
	result, err := one(context.Background(), req, lookup)
	if err != nil || string(result.Body) != "feed" || !reflect.DeepEqual(lookedUp, []string{"start.invalid", "end.invalid"}) {
		t.Fatalf("response = %+v, err = %v, lookups = %v", result, err, lookedUp)
	}
}
