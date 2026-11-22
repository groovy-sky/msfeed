package parse

import (
	"bytes"
	"fmt"
	"strings"
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

func TestParseHonorsDeclaredEncoding(t *testing.T) {
	latin1 := []byte(`<?xml version="1.0" encoding="ISO-8859-1"?><rss version="2.0"><channel><title>Encod</title><item><guid>one</guid><title>CafXX</title></item></channel></rss>`)
	latin1 = bytes.Replace(latin1, []byte{0x58, 0x58}, []byte{0xe9, 0x21}, 1)
	result, err := Parse(latin1, "https://example.com/feed", "encoding", "snapshot", time.Unix(0, 0), 10)
	if err != nil {
		t.Fatal(err)
	}
	if result.Entries[0].Title != "Café!" {
		t.Fatalf("title = %q", result.Entries[0].Title)
	}
}

func TestRecordUsesCompactUnixTimestampAndPlainSummary(t *testing.T) {
	body := `<?xml version="1.0"?><rss version="2.0"><channel><title>News</title>` +
		`<item><guid>compact</guid><title>Compact item</title>` +
		`<link>/compact</link><pubDate>Thu, 12 Nov 2026 16:00:02 +0000</pubDate>` +
		`<description><![CDATA[<p>First <strong>important</strong> update.</p>` +
		`<p>The post Compact item appeared first on Microsoft.</p>]]></description></item>` +
		`</channel></rss>`
	parsed, err := Parse([]byte(body), "https://example.com/feed", "compact", "snapshot", time.Unix(0, 0), 1)
	if err != nil {
		t.Fatal(err)
	}
	record := parsed.Entries[0].Record()
	if record.URL != "https://example.com/compact" ||
		record.Title != "Compact item" ||
		record.PublishedAt != 1794499202 ||
		record.Summary != "First important update." {
		t.Fatalf("record = %#v", record)
	}
	if record.Hash() != parsed.Entries[0].ContentHash {
		t.Fatalf("record hash = %s, content hash = %s", record.Hash(), parsed.Entries[0].ContentHash)
	}
}

func TestRecordUsesUpdatedDateWhenPublishedDateIsMissing(t *testing.T) {
	record := (Entry{UpdatedAt: "2025-01-02T03:04:05Z"}).Record()
	if record.PublishedAt != 1735787045 {
		t.Fatalf("published_at = %d, want updated timestamp", record.PublishedAt)
	}
}

func TestCompactSummaryDoesNotRepeatEllipsis(t *testing.T) {
	value := strings.Repeat("word ", MaxStoredSummaryRunes/5) + "ending..."
	record := (Entry{Summary: value}).Record()
	if strings.HasSuffix(record.Summary, "....") {
		t.Fatalf("summary repeats ellipsis: %q", record.Summary)
	}
	if len([]rune(record.Summary)) > MaxStoredSummaryRunes {
		t.Fatalf("summary length = %d, want <= %d", len([]rune(record.Summary)), MaxStoredSummaryRunes)
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

func TestParsePreservesBaseContentTypeAndExtensions(t *testing.T) {
	body := `<?xml version="1.0"?><feed xmlns="http://www.w3.org/2005/Atom"
		xml:base="https://example.com/articles/" xmlns:media="http://search.yahoo.com/mrss/">
		<title>News</title><entry xml:base="nested/"><id>stable</id><title>Entry</title>
		<updated>not-a-date</updated><content type="text/html">&lt;p&gt;Body&lt;/p&gt;</content>
		<link rel="alternate" href="post.html"/><media:thumbnail url="/thumbnail.jpg"/>
		</entry></feed>`
	result, err := Parse([]byte(body), "https://example.com/feed", "test", "snapshot", time.Unix(0, 0), 10)
	if err != nil {
		t.Fatal(err)
	}
	entry := result.Entries[0]
	if entry.URL != "https://example.com/articles/nested/post.html" {
		t.Fatalf("URL = %q", entry.URL)
	}
	if entry.ContentType != "text/html" {
		t.Fatalf("content type = %q", entry.ContentType)
	}
	if entry.RawUpdatedDate != "not-a-date" || entry.UpdatedAt != "" {
		t.Fatalf("invalid date was not preserved: raw=%q normalized=%q", entry.RawUpdatedDate, entry.UpdatedAt)
	}
	if len(entry.Extensions["media"]["thumbnail"]) != 1 {
		t.Fatalf("extension = %#v", entry.Extensions)
	}
}

func TestRevisionHashIgnoresIncidentalCategoryOrder(t *testing.T) {
	template := `<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>
		<item><guid>same</guid><category>%s</category><category>%s</category></item></channel></rss>`
	first, err := Parse([]byte(fmt.Sprintf(template, "alpha", "beta")), "https://example.com/feed", "x", "a", time.Unix(0, 0), 1)
	if err != nil {
		t.Fatal(err)
	}

	second, err := Parse([]byte(fmt.Sprintf(template, "beta", "alpha")), "https://example.com/feed", "x", "b", time.Unix(0, 0), 1)
	if err != nil {
		t.Fatal(err)
	}
	if first.Entries[0].ContentHash != second.Entries[0].ContentHash {
		t.Fatal("category order changed revision hash")
	}
}

func TestParseUsesDocumentedFallbackIdentity(t *testing.T) {
	body := `<?xml version="1.0"?><rss version="2.0"><channel><title>x</title>
		<item><title>No identifier</title><description>Body</description></item>
		</channel></rss>`
	parsed, err := Parse([]byte(body), "https://example.com/feed", "fallback", "snapshot", time.Unix(0, 0), 10)
	if err != nil {
		t.Fatal(err)
	}
	entry := parsed.Entries[0]
	if entry.IdentityBasis != "fallback" || entry.EntryKey == "" {
		t.Fatalf("entry = %#v", entry)
	}
	edited := []byte(strings.Replace(body, "Body", "Changed", 1))
	changed, err := Parse(edited, "https://example.com/feed", "fallback", "snapshot", time.Unix(0, 0), 10)
	if err != nil {
		t.Fatal(err)
	}
	if changed.Entries[0].EntryKey == entry.EntryKey {
		t.Fatal("fallback identity unexpectedly masked a content edit")
	}
}

func TestParseRejectsExternalEntity(t *testing.T) {
	body := []byte(`<?xml version="1.0"?>
		<!DOCTYPE rss [<!ENTITY xxe SYSTEM "file:///etc/passwd">]>
		<rss version="2.0"><channel><title>&xxe;</title></channel></rss>`)
	if _, err := Parse(body, "https://example.com/feed", "hostile", "snapshot", time.Unix(0, 0), 10); err == nil {
		t.Fatal("external entity payload was accepted")
	}
}
