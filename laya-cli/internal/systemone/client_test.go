package systemone

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"
)

func TestParseChoicesArray(t *testing.T) {
	choices, err := ParseChoices(`["joke","story","report"]`)
	if err != nil {
		t.Fatal(err)
	}
	if choices["report"] != "report" {
		t.Fatalf("report description = %q", choices["report"])
	}
}

func TestParseChoicesDescriptions(t *testing.T) {
	choices, err := ParseChoices(`{"joke":"A humorous anecdote","report":"A factual status log"}`)
	if err != nil {
		t.Fatal(err)
	}
	if choices["joke"] != "A humorous anecdote" {
		t.Fatalf("joke description = %q", choices["joke"])
	}
}

func TestParseChoicesRejectsInvalidInput(t *testing.T) {
	for _, input := range []string{
		`[]`,
		`{}`,
		`["same","same"]`,
		`[""]`,
		`{"report":""}`,
		`{"report":42}`,
		`not-json`,
	} {
		t.Run(input, func(t *testing.T) {
			if _, err := ParseChoices(input); err == nil {
				t.Fatal("expected an error")
			}
		})
	}
}

func TestClientDecide(t *testing.T) {
	choices := map[string]string{
		"joke":   "A humorous anecdote",
		"story":  "story",
		"report": "A factual status log",
	}

	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, request *http.Request) {
		if request.URL.Path != "/v1/systemone" {
			t.Errorf("path = %q", request.URL.Path)
		}
		if request.Header.Get("Content-Type") != "application/json" {
			t.Errorf("content type = %q", request.Header.Get("Content-Type"))
		}

		var body Request
		if err := json.NewDecoder(request.Body).Decode(&body); err != nil {
			t.Errorf("decode request: %v", err)
		}
		if body.State != "backup completed successfully" {
			t.Errorf("state = %q", body.State)
		}
		question := body.Questions[decisionQuestion]
		if question.Type != "choice" {
			t.Errorf("question type = %q", question.Type)
		}
		if question.Instructions == "" {
			t.Error("instructions are empty")
		}
		if question.Criteria["report"] != "A factual status log" {
			t.Errorf("report criterion = %q", question.Criteria["report"])
		}

		response.Header().Set("Content-Type", "application/json")
		_, _ = response.Write([]byte(`{"model":"laya","answers":{"decision":{"type":"choice","choice":"report","probabilities":{"joke":0.004,"story":0.009,"report":0.987},"confidence":0.98}},"usage":{"input_tokens":12,"output_tokens":0}}`))
	}))
	defer server.Close()

	result, err := NewClient(server.URL, server.Client()).Decide(
		context.Background(),
		"backup completed successfully",
		choices,
	)
	if err != nil {
		t.Fatal(err)
	}
	if result.Choice != "report" || result.Probability != 0.987 {
		t.Fatalf("result = %#v", result)
	}
	if result.Margin != 0.978 {
		t.Fatalf("margin = %f", result.Margin)
	}
}

func TestRankRejectsIncompleteProbabilities(t *testing.T) {
	_, err := Rank(
		map[string]float64{"report": 1},
		map[string]string{"report": "report", "story": "story"},
	)
	if err == nil {
		t.Fatal("expected an error")
	}
}

func TestClientDecideRejectsServerError(t *testing.T) {
	server := httptest.NewServer(http.HandlerFunc(func(response http.ResponseWriter, _ *http.Request) {
		http.Error(response, "model unavailable", http.StatusServiceUnavailable)
	}))
	defer server.Close()

	_, err := NewClient(server.URL, server.Client()).Decide(
		context.Background(),
		"state",
		map[string]string{"report": "report"},
	)
	if err == nil {
		t.Fatal("expected an error")
	}
}
