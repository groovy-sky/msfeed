package systemone

import (
	"bytes"
	"context"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"math"
	"net/http"
	"strings"
)

const decisionQuestion = "decision"

type Request struct {
	State     string                    `json:"state"`
	Questions map[string]ChoiceQuestion `json:"questions"`
}

type ChoiceQuestion struct {
	Type         string            `json:"type"`
	Instructions string            `json:"instructions"`
	Criteria     map[string]string `json:"criteria"`
}

type Response struct {
	Answers map[string]ChoiceAnswer `json:"answers"`
}

type ChoiceAnswer struct {
	Probabilities map[string]float64 `json:"probabilities"`
}

type Result struct {
	Choice        string
	Probability   float64
	Probabilities map[string]float64
	Margin        float64
}

type Client struct {
	baseURL    string
	httpClient *http.Client
}

func NewClient(baseURL string, httpClient *http.Client) *Client {
	if httpClient == nil {
		httpClient = http.DefaultClient
	}
	return &Client{
		baseURL:    strings.TrimRight(baseURL, "/"),
		httpClient: httpClient,
	}
}

func ParseChoices(input string) (map[string]string, error) {
	var labels []string
	if err := json.Unmarshal([]byte(input), &labels); err == nil {
		choices := make(map[string]string, len(labels))
		for _, label := range labels {
			if strings.TrimSpace(label) == "" {
				return nil, errors.New("choice labels must not be empty")
			}
			if _, exists := choices[label]; exists {
				return nil, fmt.Errorf("duplicate choice label %q", label)
			}
			choices[label] = label
		}
		if len(choices) == 0 {
			return nil, errors.New("choices must contain at least one label")
		}
		return choices, nil
	}

	var choices map[string]string
	if err := json.Unmarshal([]byte(input), &choices); err != nil {
		return nil, errors.New("choices must be a JSON array of strings or an object of string descriptions")
	}
	if len(choices) == 0 {
		return nil, errors.New("choices must contain at least one label")
	}
	for label, description := range choices {
		if strings.TrimSpace(label) == "" {
			return nil, errors.New("choice labels must not be empty")
		}
		if strings.TrimSpace(description) == "" {
			return nil, fmt.Errorf("description for choice %q must not be empty", label)
		}
	}
	return choices, nil
}

func (c *Client) Decide(ctx context.Context, state string, choices map[string]string) (Result, error) {
	requestBody, err := json.Marshal(Request{
		State: state,
		Questions: map[string]ChoiceQuestion{
			decisionQuestion: {
				Type:         "choice",
				Instructions: "Choose the option that best matches the state.",
				Criteria:     choices,
			},
		},
	})
	if err != nil {
		return Result{}, fmt.Errorf("encode decision request: %w", err)
	}

	request, err := http.NewRequestWithContext(
		ctx,
		http.MethodPost,
		c.baseURL+"/v1/systemone",
		bytes.NewReader(requestBody),
	)
	if err != nil {
		return Result{}, fmt.Errorf("create decision request: %w", err)
	}
	request.Header.Set("Content-Type", "application/json")
	request.Header.Set("Accept", "application/json")

	response, err := c.httpClient.Do(request)
	if err != nil {
		return Result{}, fmt.Errorf("send decision request: %w", err)
	}
	defer response.Body.Close()

	if response.StatusCode < http.StatusOK || response.StatusCode >= http.StatusMultipleChoices {
		body, _ := io.ReadAll(io.LimitReader(response.Body, 4096))
		return Result{}, fmt.Errorf("decision endpoint returned %s: %s", response.Status, strings.TrimSpace(string(body)))
	}

	var decoded Response
	decoder := json.NewDecoder(response.Body)
	if err := decoder.Decode(&decoded); err != nil {
		return Result{}, fmt.Errorf("decode decision response: %w", err)
	}

	answer, ok := decoded.Answers[decisionQuestion]
	if !ok {
		return Result{}, fmt.Errorf("decision response is missing answer %q", decisionQuestion)
	}
	return Rank(answer.Probabilities, choices)
}

func Rank(probabilities map[string]float64, choices map[string]string) (Result, error) {
	if len(probabilities) == 0 {
		return Result{}, errors.New("decision response contains no probabilities")
	}

	ranked := make([]struct {
		label       string
		probability float64
	}, 0, len(choices))
	normalized := make(map[string]float64, len(choices))
	for label := range choices {
		probability, ok := probabilities[label]
		if !ok {
			return Result{}, fmt.Errorf("decision response is missing probability for choice %q", label)
		}
		if math.IsNaN(probability) || math.IsInf(probability, 0) || probability < 0 || probability > 1 {
			return Result{}, fmt.Errorf("probability for choice %q must be between 0 and 1", label)
		}
		normalized[label] = probability
		ranked = append(ranked, struct {
			label       string
			probability float64
		}{label: label, probability: probability})
	}
	for label := range probabilities {
		if _, ok := choices[label]; !ok {
			return Result{}, fmt.Errorf("decision response contains unknown choice %q", label)
		}
	}

	for i := 0; i < len(ranked); i++ {
		for j := i + 1; j < len(ranked); j++ {
			if ranked[j].probability > ranked[i].probability ||
				(ranked[j].probability == ranked[i].probability && ranked[j].label < ranked[i].label) {
				ranked[i], ranked[j] = ranked[j], ranked[i]
			}
		}
	}

	result := Result{
		Choice:        ranked[0].label,
		Probability:   ranked[0].probability,
		Probabilities: normalized,
	}
	if len(ranked) > 1 {
		result.Margin = ranked[0].probability - ranked[1].probability
	}
	return result, nil
}
