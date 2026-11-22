package enrich

import (
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"net/url"
	"strconv"
	"strings"
	"time"

	"github.com/groovy-sky/msfeed/internal/fetch"
	"github.com/groovy-sky/msfeed/internal/parse"
)

const maxFilterRunes = 6000
const maxSourceIDRunes = 512

type Candidate struct {
	SourceID string
}

type Options struct {
	Provider           string
	URL                string
	IDField            string
	ContentField       string
	ContentType        string
	MaxItemsPerRequest int
}

type HTTPOptions struct {
	Timeout             time.Duration
	MaxBodyBytes        int64
	MaxRedirects        int
	UserAgent           string
	AllowPrivateNetwork bool
	PublicDNSFallback   bool
}

type Result struct {
	Content  map[string]string
	Requests int
	Deferred int
	Warnings []string
}

func OData(
	ctx context.Context,
	options Options,
	httpOptions HTTPOptions,
	candidates []Candidate,
	requestBudget int,
) Result {
	if options.Provider != "odata" {
		return Result{Deferred: len(candidates), Warnings: []string{
			fmt.Sprintf("unsupported content enrichment provider %q", options.Provider),
		}}
	}
	ids, skipped := uniqueSourceIDs(candidates)
	result := Result{Content: make(map[string]string)}
	if skipped > 0 {
		result.Warnings = append(result.Warnings, fmt.Sprintf(
			"skipped %d entry ID(s) that could not be used safely with the enrichment provider", skipped,
		))
	}
	remainingRequests := requestBudget
	processed := 0
	for processed < len(ids) {
		if remainingRequests <= 0 {
			result.Deferred = len(ids) - processed
			break
		}
		batch := nextBatch(ids[processed:], options.MaxItemsPerRequest)
		endpoint, err := batchURL(options, batch)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("build content enrichment request: %v", err))
			result.Deferred += len(batch)
			processed += len(batch)
			continue
		}
		maxRedirects := httpOptions.MaxRedirects
		if allowed := remainingRequests - 1; maxRedirects > allowed {
			maxRedirects = allowed
		}
		fetched, err := fetch.One(ctx, fetch.Request{
			URL: endpoint, Timeout: httpOptions.Timeout,
			MaxBodyBytes: httpOptions.MaxBodyBytes, MaxRedirects: maxRedirects,
			AllowPrivateNetwork: httpOptions.AllowPrivateNetwork,
			PublicDNSFallback:   httpOptions.PublicDNSFallback,
			UserAgent:           httpOptions.UserAgent,
			Headers:             map[string]string{"Accept": "application/json"},
		})
		used := 1 + len(fetched.Redirects)
		result.Requests += used
		remainingRequests -= used
		processed += len(batch)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("fetch enriched content: %v", err))
			result.Deferred += len(batch)
			continue
		}
		values, err := decodeValues(fetched.DecodedBody, options, batch)
		if err != nil {
			result.Warnings = append(result.Warnings, fmt.Sprintf("decode enriched content: %v", err))
			result.Deferred += len(batch)
			continue
		}
		for id, content := range values {
			result.Content[id] = content
		}
	}
	return result
}

func uniqueSourceIDs(candidates []Candidate) ([]string, int) {
	seen := make(map[string]struct{}, len(candidates))
	ids := make([]string, 0, len(candidates))
	skipped := 0
	for _, candidate := range candidates {
		id := strings.TrimSpace(candidate.SourceID)
		if id == "" || len([]rune(id)) > maxSourceIDRunes {
			skipped++
			continue
		}
		if _, ok := seen[id]; ok {
			continue
		}
		seen[id] = struct{}{}
		ids = append(ids, id)
	}
	return ids, skipped
}

func nextBatch(ids []string, maximum int) []string {
	if maximum <= 0 {
		maximum = 100
	}
	size := 0
	filterSize := 0
	for size < len(ids) && size < maximum {
		term := len([]rune("id eq '" + odataString(ids[size]) + "'"))
		separator := 0
		if size > 0 {
			separator = len(" or ")
		}
		if size > 0 && filterSize+separator+term > maxFilterRunes {
			break
		}
		filterSize += separator + term
		size++
	}
	if size == 0 && len(ids) > 0 {
		size = 1
	}
	return ids[:size]
}

func batchURL(options Options, ids []string) (string, error) {
	endpoint, err := url.Parse(options.URL)
	if err != nil {
		return "", err
	}
	idField := options.IDField
	if idField == "" {
		idField = "id"
	}
	contentField := options.ContentField
	if contentField == "" {
		contentField = "description"
	}
	terms := make([]string, len(ids))
	for index, id := range ids {
		terms[index] = idField + " eq '" + odataString(id) + "'"
	}
	query := endpoint.Query()
	query.Set("$filter", strings.Join(terms, " or "))
	query.Set("$select", idField+","+contentField)
	query.Set("$top", strconv.Itoa(len(ids)))
	endpoint.RawQuery = query.Encode()
	return endpoint.String(), nil
}

func odataString(value string) string {
	return strings.ReplaceAll(value, "'", "''")
}

func decodeValues(body []byte, options Options, requested []string) (map[string]string, error) {
	var response struct {
		Value []map[string]json.RawMessage `json:"value"`
	}
	decoder := json.NewDecoder(strings.NewReader(string(body)))
	if err := decoder.Decode(&response); err != nil {
		return nil, err
	}
	if response.Value == nil {
		return nil, errors.New("response is missing value array")
	}
	allowed := make(map[string]struct{}, len(requested))
	for _, id := range requested {
		allowed[id] = struct{}{}
	}
	idField := options.IDField
	if idField == "" {
		idField = "id"
	}
	contentField := options.ContentField
	if contentField == "" {
		contentField = "description"
	}
	contentType := options.ContentType
	if contentType == "" {
		contentType = "text/html"
	}
	values := make(map[string]string, len(response.Value))
	for _, item := range response.Value {
		id, err := rawString(item[idField])
		if err != nil || id == "" {
			continue
		}
		if _, ok := allowed[id]; !ok {
			continue
		}
		content, err := rawString(item[contentField])
		if err != nil {
			continue
		}
		content = parse.PlainText(content, contentType)
		if content != "" {
			values[id] = content
		}
	}
	return values, nil
}

func rawString(raw json.RawMessage) (string, error) {
	if len(raw) == 0 || string(raw) == "null" {
		return "", errors.New("missing JSON value")
	}
	var text string
	if err := json.Unmarshal(raw, &text); err == nil {
		return text, nil
	}
	decoder := json.NewDecoder(strings.NewReader(string(raw)))
	decoder.UseNumber()
	var value any
	if err := decoder.Decode(&value); err != nil {
		return "", err
	}
	switch typed := value.(type) {
	case json.Number:
		return typed.String(), nil
	default:
		return "", errors.New("JSON value is not a string or number")
	}
}
