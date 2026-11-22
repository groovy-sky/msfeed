package enrich

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"net/url"
	"strings"
	"sync"
	"testing"
	"time"
)

func TestODataBatchesAndCleansContent(t *testing.T) {
	var mu sync.Mutex
	var requests []*http.Request
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		mu.Lock()
		requests = append(requests, request.Clone(context.Background()))
		call := len(requests)
		mu.Unlock()
		w.Header().Set("Content-Type", "application/json")
		switch call {
		case 1:
			_, _ = fmt.Fprint(w, `{"value":[
				{"id":"one","description":"<p>First &amp; complete.</p><script>ignored()</script>"},
				{"id":"two","description":"<div>Second complete.</div>"}
			]}`)
		case 2:
			_, _ = fmt.Fprint(w, `{"value":[{"id":"three","description":"Third complete."}]}`)
		default:
			http.Error(w, "unexpected request", http.StatusInternalServerError)
		}
	}))
	defer server.Close()

	result := OData(
		context.Background(),
		Options{
			Provider: "odata", URL: server.URL + "/api?existing=true",
			IDField: "id", ContentField: "description", ContentType: "text/html",
			MaxItemsPerRequest: 2,
		},
		HTTPOptions{
			Timeout: time.Second, MaxBodyBytes: 1 << 20, MaxRedirects: 1,
			UserAgent: "enrich-test", AllowPrivateNetwork: true,
		},
		[]Candidate{{SourceID: "one"}, {SourceID: "two"}, {SourceID: "three"}},
		2,
	)
	if result.Requests != 2 || result.Deferred != 0 || len(result.Warnings) != 0 {
		t.Fatalf("result = %+v", result)
	}
	if result.Content["one"] != "First & complete." ||
		result.Content["two"] != "Second complete." ||
		result.Content["three"] != "Third complete." {
		t.Fatalf("content = %#v", result.Content)
	}
	if len(requests) != 2 {
		t.Fatalf("requests = %d, want 2", len(requests))
	}
	for _, request := range requests {
		if request.Header.Get("Accept") != "application/json" {
			t.Fatalf("Accept = %q", request.Header.Get("Accept"))
		}
		query := request.URL.Query()
		if query.Get("existing") != "true" ||
			query.Get("$select") != "id,description" ||
			!strings.Contains(query.Get("$filter"), "id eq") {
			t.Fatalf("query = %s", request.URL.RawQuery)
		}
	}
}

func TestODataHonorsRequestBudgetAndEscapesIDs(t *testing.T) {
	var rawQuery string
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, request *http.Request) {
		rawQuery = request.URL.RawQuery
		_, _ = fmt.Fprint(w, `{"value":[{"id":"O'Neil","description":"Complete"}]}`)
	}))
	defer server.Close()

	result := OData(
		context.Background(),
		Options{
			Provider: "odata", URL: server.URL, ContentField: "description",
			MaxItemsPerRequest: 1,
		},
		HTTPOptions{
			Timeout: time.Second, MaxBodyBytes: 1 << 20,
			UserAgent: "enrich-test", AllowPrivateNetwork: true,
		},
		[]Candidate{{SourceID: "O'Neil"}, {SourceID: "later"}},
		1,
	)
	if result.Requests != 1 || result.Deferred != 1 || result.Content["O'Neil"] != "Complete" {
		t.Fatalf("result = %+v", result)
	}
	decoded, err := url.QueryUnescape(rawQuery)
	if err != nil {
		t.Fatal(err)
	}
	if !strings.Contains(decoded, "id eq 'O''Neil'") {
		t.Fatalf("filter did not escape ID: %s", decoded)
	}
}

func TestODataReportsInvalidResponseWithoutFailingCollection(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
		_, _ = fmt.Fprint(w, `{"value":`)
	}))
	defer server.Close()

	result := OData(
		context.Background(),
		Options{Provider: "odata", URL: server.URL},
		HTTPOptions{
			Timeout: time.Second, MaxBodyBytes: 1 << 20,
			UserAgent: "enrich-test", AllowPrivateNetwork: true,
		},
		[]Candidate{{SourceID: "one"}},
		1,
	)
	if result.Requests != 1 || result.Deferred != 1 || len(result.Warnings) == 0 {
		t.Fatalf("result = %+v", result)
	}
}
