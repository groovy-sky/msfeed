package parse

import (
	"testing"
	"time"
)

func TestParseSupportedFeedFormats(t *testing.T) {
	t.Parallel()
	tests := []struct {
		name       string
		body       string
		wantFormat string
	}{
		{
			name: "rss2",
			body: `<?xml version="1.0"?><rss version="2.0"><channel><title>News</title>` +
				`<link>https://example.com/</link><item><guid>item-1</guid><title>First</title>` +
				`<link>/posts/1</link><pubDate>Tue, 10 Jun 2003 04:00:00 GMT</pubDate></item></channel></rss>`,
			wantFormat: "rss",
		},
		{
			name: "atom",
			body: `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"><title>News</title>` +
				`<entry><id>tag:example.com,2025:1</id><title>Atom item</title><link href="/atom/1"/>` +
				`<updated>2025-01-02T03:04:05Z</updated></entry></feed>`,
			wantFormat: "atom",
		},
		{
			name: "rss1",
			body: `<?xml version="1.0"?><rdf:RDF xmlns:rdf="http://www.w3.org/1999/02/22-rdf-syntax-ns#"` +
				` xmlns="http://purl.org/rss/1.0/"><channel rdf:about="https://example.com/"><title>RDF</title>` +
				`<link>https://example.com/</link></channel><item rdf:about="https://example.com/rdf/1">` +
				`<title>RDF item</title><link>/rdf/1</link></item></rdf:RDF>`,
			wantFormat: "rss",
		},
	}
	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			result, err := Parse([]byte(tt.body), "https://example.com/feed", "test", "snapshot", time.Unix(0, 0), 100)
			if err != nil {
				t.Fatal(err)
			}
			if result.Format != tt.wantFormat {
				t.Fatalf("format = %q, want %q", result.Format, tt.wantFormat)
			}
			if len(result.Entries) != 1 {
				t.Fatalf("entries = %d, want 1", len(result.Entries))
			}
			entry := result.Entries[0]
			if entry.EntryKey == "" || entry.ContentHash == "" {
				t.Fatal("identity and content hashes must be populated")
			}
			if entry.URL == "" || entry.URL[:4] != "http" {
				t.Fatalf("resolved URL = %q", entry.URL)
			}
		})
	}
}

func TestParseIsDeterministicAcrossReordering(t *testing.T) {
	a := `<?xml version="1.0"?><rss version="2.0"><channel><title>x</title><item><guid>1</guid><title>A</title></item><item><guid>2</guid><title>B</title></item></channel></rss>`
	b := `<?xml version="1.0"?><rss version="2.0"><channel><title>x</title><item><guid>2</guid><title>B</title></item><item><guid>1</guid><title>A</title></item></channel></rss>`
	first, err := Parse([]byte(a), "https://example.com/feed", "feed", "snapshot", time.Unix(1, 0), 100)
	if err != nil {
		t.Fatal(err)
	}
	second, err := Parse([]byte(b), "https://example.com/feed", "feed", "snapshot", time.Unix(2, 0), 100)
	if err != nil {
		t.Fatal(err)
	}
	keys := make(map[string]string)
	for _, entry := range first.Entries {
		keys[entry.EntryKey] = entry.ContentHash
	}
	for _, entry := range second.Entries {
		if keys[entry.EntryKey] != entry.ContentHash {
			t.Fatalf("entry %s changed hash after reordering", entry.EntryKey)
		}
	}
}
