package parse

import (
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"net/url"
	"strings"
	"time"

	"github.com/mmcdole/gofeed"
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

type Entry struct {
	SchemaVersion    int               `json:"schema_version"`
	EntryKey         string            `json:"entry_key"`
	FeedID           string            `json:"feed_id"`
	IdentityBasis    string            `json:"identity_basis"`
	SourceID         string            `json:"source_id,omitempty"`
	Title            string            `json:"title,omitempty"`
	URL              string            `json:"url,omitempty"`
	Authors          []string          `json:"authors"`
	PublishedAt      string            `json:"published_at,omitempty"`
	UpdatedAt        string            `json:"updated_at,omitempty"`
	RawPublishedDate string            `json:"raw_published_date,omitempty"`
	RawUpdatedDate   string            `json:"raw_updated_date,omitempty"`
	Summary          string            `json:"summary,omitempty"`
	Content          string            `json:"content,omitempty"`
	ContentType      string            `json:"content_type,omitempty"`
	Categories       []string          `json:"categories"`
	Attachments      []Attachment      `json:"attachments"`
	Extensions       map[string]string `json:"extensions"`
	ContentHash      string            `json:"content_hash"`
	Provenance       Provenance        `json:"provenance"`
}

type Feed struct {
	Format      string
	Title       string
	Link        string
	Description string
	Entries     []Entry
	EntryLimit  int
}

type revisionContent struct {
	IdentityBasis    string            `json:"identity_basis"`
	SourceID         string            `json:"source_id,omitempty"`
	Title            string            `json:"title,omitempty"`
	URL              string            `json:"url,omitempty"`
	Authors          []string          `json:"authors"`
	PublishedAt      string            `json:"published_at,omitempty"`
	UpdatedAt        string            `json:"updated_at,omitempty"`
	RawPublishedDate string            `json:"raw_published_date,omitempty"`
	RawUpdatedDate   string            `json:"raw_updated_date,omitempty"`
	Summary          string            `json:"summary,omitempty"`
	Content          string            `json:"content,omitempty"`
	ContentType      string            `json:"content_type,omitempty"`
	Categories       []string          `json:"categories"`
	Attachments      []Attachment      `json:"attachments"`
	Extensions       map[string]string `json:"extensions"`
}

func Parse(body []byte, baseURL, feedID, snapshotHash string, retrievedAt time.Time, maxEntries int) (*Feed, error) {
	parsed, err := gofeed.NewParser().ParseString(string(body))
	if err != nil {
		return nil, fmt.Errorf("parse feed: %w", err)
	}
	format := parsed.FeedType
	if format == "" {
		format = "unknown"
	}
	entries := parsed.Items
	limit := maxEntries
	if limit > 0 && len(entries) > limit {
		entries = entries[:limit]
	}
	result := &Feed{
		Format:      format,
		Title:       parsed.Title,
		Link:        resolve(baseURL, parsed.Link),
		Description: parsed.Description,
		EntryLimit:  len(parsed.Items) - len(entries),
		Entries:     make([]Entry, 0, len(entries)),
	}
	for _, item := range entries {
		entry := normalize(item, feedID, baseURL, snapshotHash, retrievedAt)
		result.Entries = append(result.Entries, entry)
	}
	return result, nil
}

func normalize(item *gofeed.Item, feedID, baseURL, snapshotHash string, retrievedAt time.Time) Entry {
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
				URL: resolve(baseURL, enclosure.URL), Type: enclosure.Type, Length: enclosure.Length,
			})
		}
	}

	identityBasis, identity := identityFor(item)
	entry := Entry{
		SchemaVersion:    SchemaVersion,
		EntryKey:         hashStrings(feedID, identityBasis, identity),
		FeedID:           feedID,
		IdentityBasis:    identityBasis,
		SourceID:         item.GUID,
		Title:            strings.TrimSpace(item.Title),
		URL:              resolve(baseURL, item.Link),
		Authors:          authors,
		PublishedAt:      normalizedTime(item.PublishedParsed),
		UpdatedAt:        normalizedTime(item.UpdatedParsed),
		RawPublishedDate: item.Published,
		RawUpdatedDate:   item.Updated,
		Summary:          item.Description,
		Content:          item.Content,
		ContentType:      "",
		Categories:       categories,
		Attachments:      attachments,
		Extensions:       copyMap(item.Custom),
		Provenance: Provenance{
			SnapshotHash: snapshotHash,
			RetrievedAt:  retrievedAt.UTC().Format(time.RFC3339),
			SourceURL:    baseURL,
		},
	}

	contentBytes, err := json.Marshal(revisionContent{
		IdentityBasis: entry.IdentityBasis, SourceID: entry.SourceID,
		Title: entry.Title, URL: entry.URL, Authors: entry.Authors,
		PublishedAt: entry.PublishedAt, UpdatedAt: entry.UpdatedAt,
		RawPublishedDate: entry.RawPublishedDate, RawUpdatedDate: entry.RawUpdatedDate,
		Summary: entry.Summary, Content: entry.Content, ContentType: entry.ContentType,
		Categories: entry.Categories, Attachments: entry.Attachments, Extensions: entry.Extensions,
	})
	if err != nil {
		panic("JSON revision content must always marshal: " + err.Error())
	}
	sum := sha256.Sum256(contentBytes)
	entry.ContentHash = hex.EncodeToString(sum[:])
	return entry
}

func copyMap(source map[string]string) map[string]string {
	result := make(map[string]string, len(source))
	for key, value := range source {
		result[key] = value
	}
	return result
}

func identityFor(item *gofeed.Item) (basis, identity string) {
	if strings.TrimSpace(item.GUID) != "" {
		return "id", item.GUID
	}
	if strings.TrimSpace(item.Link) != "" {
		return "link", item.Link
	}
	var author string
	if item.Author != nil {
		author = item.Author.Name
	}
	identity = strings.Join([]string{item.Title, item.Published, item.Updated, author, item.Description, item.Content}, "\x1f")
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
