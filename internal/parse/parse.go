package parse

import (
	"bytes"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"encoding/xml"
	"fmt"
	"io"
	"net/url"
	"sort"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
	ext "github.com/mmcdole/gofeed/extensions"
	"golang.org/x/net/html/charset"
)

const SchemaVersion = 1

type Attachment struct {
	URL    string `json:"url"`
	Type   string `json:"type,omitempty"`
	Length string `json:"length,omitempty"`
}

type Provenance struct {
	SnapshotHash string `json:"snapshot_hash"`
	RetrievedAt  string `json:"retrieved_at"`
	SourceURL    string `json:"source_url"`
}

type Extension struct {
	Name     string                 `json:"name"`
	Value    string                 `json:"value,omitempty"`
	Attrs    map[string]string      `json:"attrs,omitempty"`
	Children map[string][]Extension `json:"children,omitempty"`
}

type Extensions map[string]map[string][]Extension

type Entry struct {
	SchemaVersion    int          `json:"schema_version"`
	EntryKey         string       `json:"entry_key"`
	FeedID           string       `json:"feed_id"`
	IdentityBasis    string       `json:"identity_basis"`
	SourceID         string       `json:"source_id,omitempty"`
	Title            string       `json:"title,omitempty"`
	URL              string       `json:"url,omitempty"`
	Authors          []string     `json:"authors"`
	PublishedAt      string       `json:"published_at,omitempty"`
	UpdatedAt        string       `json:"updated_at,omitempty"`
	RawPublishedDate string       `json:"raw_published_date,omitempty"`
	RawUpdatedDate   string       `json:"raw_updated_date,omitempty"`
	Summary          string       `json:"summary,omitempty"`
	Content          string       `json:"content,omitempty"`
	ContentType      string       `json:"content_type,omitempty"`
	Categories       []string     `json:"categories"`
	Attachments      []Attachment `json:"attachments"`
	Extensions       Extensions   `json:"extensions"`
	ContentHash      string       `json:"content_hash"`
	Provenance       Provenance   `json:"provenance"`
}

type Feed struct {
	Format           string
	Title            string
	Link             string
	Description      string
	Entries          []Entry
	TotalEntries     int
	Offset           int
	RemainingEntries int
	Warnings         []string
}

type revisionContent struct {
	IdentityBasis    string       `json:"identity_basis"`
	SourceID         string       `json:"source_id,omitempty"`
	Title            string       `json:"title,omitempty"`
	URL              string       `json:"url,omitempty"`
	Authors          []string     `json:"authors"`
	PublishedAt      string       `json:"published_at,omitempty"`
	UpdatedAt        string       `json:"updated_at,omitempty"`
	RawPublishedDate string       `json:"raw_published_date,omitempty"`
	RawUpdatedDate   string       `json:"raw_updated_date,omitempty"`
	Summary          string       `json:"summary,omitempty"`
	Content          string       `json:"content,omitempty"`
	ContentType      string       `json:"content_type,omitempty"`
	Categories       []string     `json:"categories"`
	Attachments      []Attachment `json:"attachments"`
	Extensions       Extensions   `json:"extensions"`
}

func Parse(body []byte, baseURL, feedID, snapshotHash string, retrievedAt time.Time, maxEntries int) (*Feed, error) {
	return ParseRange(body, baseURL, feedID, snapshotHash, retrievedAt, 0, maxEntries)
}

func ParseRange(body []byte, baseURL, feedID, snapshotHash string, retrievedAt time.Time, offset, maxEntries int) (*Feed, error) {
	parsed, err := gofeed.NewParser().ParseString(string(body))
	if err != nil {
		return nil, fmt.Errorf("parse feed: %w", err)
	}
	metadata, err := inspectXML(body, baseURL)
	if err != nil {
		return nil, fmt.Errorf("inspect feed XML: %w", err)
	}
	if offset < 0 || offset > len(parsed.Items) {
		return nil, fmt.Errorf("entry offset %d is outside parsed range of %d", offset, len(parsed.Items))
	}
	format := parsed.FeedType
	if format == "" {
		format = "unknown"
	}
	end := len(parsed.Items)
	entries := parsed.Items[offset:]
	limit := maxEntries
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	result := &Feed{
		Format:           format,
		Title:            parsed.Title,
		Link:             resolve(baseURL, parsed.Link),
		Description:      parsed.Description,
		Entries:          make([]Entry, 0, len(entries)),
		TotalEntries:     len(parsed.Items),
		Offset:           offset,
		RemainingEntries: end - offset - len(entries),
		Warnings:         make([]string, 0),
	}
	if len(metadata) != len(parsed.Items) {
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"XML metadata count %d differs from normalized entry count %d", len(metadata), len(parsed.Items),
		))
	}
	for index, item := range entries {
		meta := entryMetadata{baseURL: baseURL}
		if rawIndex := offset + index; rawIndex < len(metadata) {
			meta = metadata[rawIndex]
		}
		entry := normalize(item, feedID, baseURL, meta, snapshotHash, retrievedAt)
		result.Entries = append(result.Entries, entry)
	}
	return result, nil
}

func normalize(item *gofeed.Item, feedID, sourceURL string, meta entryMetadata, snapshotHash string, retrievedAt time.Time) Entry {
	authors := make([]string, 0)
	if item.Author != nil && item.Author.Name != "" {
		authors = append(authors, item.Author.Name)
	}
	for _, author := range item.Authors {
		if author != nil && author.Name != "" {
			found := false
			for _, existing := range authors {
				if existing == author.Name {
					found = true
					break
				}
			}
			if !found {
				authors = append(authors, author.Name)
			}
		}
	}
	categories := append([]string{}, item.Categories...)
	attachments := make([]Attachment, 0, len(item.Enclosures))
	for _, enclosure := range item.Enclosures {
		if enclosure != nil {
			attachments = append(attachments, Attachment{
				URL: resolve(meta.baseURL, enclosure.URL), Type: enclosure.Type, Length: enclosure.Length,
			})
		}
	}

	identityBasis, identity := identityFor(item, meta.baseURL)
	entry := Entry{
		SchemaVersion:    SchemaVersion,
		EntryKey:         hashStrings(feedID, identityBasis, identity),
		FeedID:           feedID,
		IdentityBasis:    identityBasis,
		SourceID:         item.GUID,
		Title:            strings.TrimSpace(item.Title),
		URL:              resolve(meta.baseURL, item.Link),
		Authors:          authors,
		PublishedAt:      normalizedTime(item.PublishedParsed),
		UpdatedAt:        normalizedTime(item.UpdatedParsed),
		RawPublishedDate: item.Published,
		RawUpdatedDate:   item.Updated,
		Summary:          item.Description,
		Content:          item.Content,
		ContentType:      meta.contentType,
		Categories:       categories,
		Attachments:      attachments,
		Extensions:       normalizeExtensions(item.Extensions, item.Custom),
		Provenance: Provenance{
			SnapshotHash: snapshotHash,
			RetrievedAt:  retrievedAt.UTC().Format(time.RFC3339),
			SourceURL:    sourceURL,
		},
	}

	contentBytes, err := json.Marshal(revisionContent{
		IdentityBasis: entry.IdentityBasis, SourceID: entry.SourceID,
		Title: entry.Title, URL: entry.URL, Authors: sortedStrings(entry.Authors),
		PublishedAt: entry.PublishedAt, UpdatedAt: entry.UpdatedAt,
		RawPublishedDate: entry.RawPublishedDate, RawUpdatedDate: entry.RawUpdatedDate,
		Summary: entry.Summary, Content: entry.Content, ContentType: entry.ContentType,
		Categories: sortedStrings(entry.Categories), Attachments: sortedAttachments(entry.Attachments),
		Extensions: entry.Extensions,
	})
	if err != nil {
		panic("JSON revision content must always marshal: " + err.Error())
	}
	sum := sha256.Sum256(contentBytes)
	entry.ContentHash = hex.EncodeToString(sum[:])
	return entry
}

func normalizeExtensions(source ext.Extensions, custom map[string]string) Extensions {
	result := make(Extensions, len(source)+1)
	for namespace, values := range source {
		namespaceValues := make(map[string][]Extension, len(values))
		for name, extensions := range values {
			normalized := make([]Extension, 0, len(extensions))
			for _, extension := range extensions {
				normalized = append(normalized, normalizeExtension(extension))
			}
			namespaceValues[name] = normalized
		}
		result[namespace] = namespaceValues
	}
	if len(custom) > 0 {
		values := make(map[string][]Extension, len(custom))
		for name, value := range custom {
			values[name] = []Extension{{Name: name, Value: value}}
		}
		result["custom"] = values
	}
	return result
}

func normalizeExtension(source ext.Extension) Extension {
	children := make(map[string][]Extension, len(source.Children))
	for name, values := range source.Children {
		normalized := make([]Extension, 0, len(values))
		for _, value := range values {
			normalized = append(normalized, normalizeExtension(value))
		}
		children[name] = normalized
	}
	return Extension{
		Name: source.Name, Value: source.Value, Attrs: copyStringMap(source.Attrs), Children: children,
	}
}

func copyStringMap(source map[string]string) map[string]string {
	if len(source) == 0 {
		return map[string]string{}
	}
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func identityFor(item *gofeed.Item, baseURL string) (basis, identity string) {
	if strings.TrimSpace(item.GUID) != "" {
		return "id", item.GUID
	}
	if strings.TrimSpace(item.Link) != "" {
		return "link", resolve(baseURL, item.Link)
	}
	authors := make([]string, 0, len(item.Authors))
	for _, author := range item.Authors {
		if author != nil {
			authors = append(authors, author.Name)
		}
	}
	if item.Author != nil && item.Author.Name != "" {
		authors = append(authors, item.Author.Name)
	}
	identity = strings.Join([]string{
		item.Title, item.Published, item.Updated, strings.Join(sortedStrings(authors), "\x1e"),
		item.Description, item.Content,
	}, "\x1f")
	return "fallback", identity
}

func normalizedTime(value *time.Time) string {
	if value == nil {
		return ""
	}
	return value.UTC().Format(time.RFC3339)
}

func resolve(base, reference string) string {
	if reference == "" {
		return ""
	}
	ref, err := url.Parse(reference)
	if err != nil {
		return reference
	}
	root, err := url.Parse(base)
	if err != nil {
		return reference
	}
	return root.ResolveReference(ref).String()
}

func hashStrings(parts ...string) string {
	h := sha256.New()
	for _, part := range parts {
		_, _ = h.Write([]byte(part))
		_, _ = h.Write([]byte{0})
	}
	return hex.EncodeToString(h.Sum(nil))
}

func sortedStrings(values []string) []string {
	result := append([]string{}, values...)
	sort.Strings(result)
	return result
}

func sortedAttachments(values []Attachment) []Attachment {
	result := append([]Attachment{}, values...)
	sort.Slice(result, func(left, right int) bool {
		a, b := result[left], result[right]
		if a.URL != b.URL {
			return a.URL < b.URL
		}
		if a.Type != b.Type {
			return a.Type < b.Type
		}
		return a.Length < b.Length
	})
	return result
}

type entryMetadata struct {
	baseURL     string
	contentType string
}

func inspectXML(body []byte, documentURL string) ([]entryMetadata, error) {
	decoder := xml.NewDecoder(bytes.NewReader(body))
	decoder.CharsetReader = charset.NewReaderLabel
	type level struct {
		name string
		base string
	}
	stack := make([]level, 0, 8)
	metadata := make([]entryMetadata, 0)
	currentEntry := -1
	currentEntryDepth := 0
	for {
		token, err := decoder.Token()
		if err == io.EOF {
			return metadata, nil
		}
		if err != nil {
			return nil, err
		}
		switch typed := token.(type) {
		case xml.StartElement:
			parentBase := documentURL
			if len(stack) > 0 {
				parentBase = stack[len(stack)-1].base
			}
			elementBase := parentBase
			contentType := ""
			for _, attr := range typed.Attr {
				if attr.Name.Local == "base" && (attr.Name.Space == "http://www.w3.org/XML/1998/namespace" || attr.Name.Space == "xml") {
					elementBase = resolve(parentBase, attr.Value)
				}
				if attr.Name.Local == "type" && strings.EqualFold(typed.Name.Local, "content") {
					contentType = attr.Value
				}
			}
			local := strings.ToLower(typed.Name.Local)
			stack = append(stack, level{name: local, base: elementBase})
			if local == "item" || local == "entry" {
				metadata = append(metadata, entryMetadata{baseURL: elementBase})
				currentEntry = len(metadata) - 1
				currentEntryDepth = len(stack)
			} else if currentEntry >= 0 && len(stack) > currentEntryDepth {
				if local == "content" && contentType != "" {
					metadata[currentEntry].contentType = contentType
				} else if local == "encoded" && typed.Name.Space == "http://purl.org/rss/1.0/modules/content/" {
					metadata[currentEntry].contentType = "text/html"
				}
			}
		case xml.EndElement:
			if len(stack) > 0 {
				closingDepth := len(stack)
				stack = stack[:len(stack)-1]
				if currentEntry >= 0 && closingDepth == currentEntryDepth &&
					(strings.EqualFold(typed.Name.Local, "item") || strings.EqualFold(typed.Name.Local, "entry")) {
					currentEntry = -1
				}
			}
		}
	}
}
