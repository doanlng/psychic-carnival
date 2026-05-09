package main

import (
	"context"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"time"
)

const (
	baseURL  = "https://api.open.fec.gov/v1"
	dbPath   = "fec_data.db"
	pageSize = 100
)

// Client wraps the OpenFEC HTTP client.
type Client struct {
	apiKey     string
	httpClient *http.Client
}

func NewClient(apiKey string) *Client {
	return &Client{
		apiKey:     apiKey,
		httpClient: &http.Client{Timeout: 30 * time.Second},
	}
}

// --- API response types ---

type Pagination struct {
	Count   int `json:"count"`
	Page    int `json:"page"`
	Pages   int `json:"pages"`
	PerPage int `json:"per_page"`
}

type CandidatesResponse struct {
	Results    []Candidate `json:"results"`
	Pagination Pagination  `json:"pagination"`
}

type Candidate struct {
	CandidateID  string `json:"candidate_id"`
	Name         string `json:"name"`
	Party        string `json:"party"`
	State        string `json:"state"`
	Office       string `json:"office"`
	ElectionYear int    `json:"election_year"`
}

type FilingsResponse struct {
	Results    []Filing   `json:"results"`
	Pagination Pagination `json:"pagination"`
}

type Filing struct {
	FilingID           int     `json:"filing_id"`
	CommitteeID        string  `json:"committee_id"`
	FormType           string  `json:"form_type"`
	ReceiptDate        string  `json:"receipt_date"`
	TotalReceipts      float64 `json:"total_receipts"`
	TotalDisbursements float64 `json:"total_disbursements"`
}

// --- Fetch helpers ---

func (c *Client) get(ctx context.Context, endpoint string, params url.Values) (*http.Response, error) {
	params.Set("api_key", c.apiKey)
	params.Set("per_page", fmt.Sprintf("%d", pageSize))

	u := fmt.Sprintf("%s%s?%s", baseURL, endpoint, params.Encode())
	req, err := http.NewRequestWithContext(ctx, http.MethodGet, u, nil)
	if err != nil {
		return nil, err
	}
	return c.httpClient.Do(req)
}

func (c *Client) FetchCandidates(ctx context.Context, electionYear int) ([]Candidate, error) {
	var all []Candidate
	page := 1

	for {
		params := url.Values{
			"election_year": {fmt.Sprintf("%d", electionYear)},
			"page":          {fmt.Sprintf("%d", page)},
		}
		resp, err := c.get(ctx, "/candidates/", params)
		if err != nil {
			return nil, fmt.Errorf("fetch candidates page %d: %w", page, err)
		}
		defer resp.Body.Close()

		var result CandidatesResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode candidates: %w", err)
		}

		all = append(all, result.Results...)
		log.Printf("candidates: fetched page %d/%d (%d total so far)", page, result.Pagination.Pages, len(all))

		if page >= result.Pagination.Pages {
			break
		}
		page++
	}

	return all, nil
}

func (c *Client) FetchFilings(ctx context.Context, committeeID string) ([]Filing, error) {
	var all []Filing
	page := 1

	for {
		params := url.Values{
			"committee_id": {committeeID},
			"page":         {fmt.Sprintf("%d", page)},
		}
		resp, err := c.get(ctx, "/filings/", params)
		if err != nil {
			return nil, fmt.Errorf("fetch filings page %d: %w", page, err)
		}
		defer resp.Body.Close()

		var result FilingsResponse
		if err := json.NewDecoder(resp.Body).Decode(&result); err != nil {
			return nil, fmt.Errorf("decode filings: %w", err)
		}

		all = append(all, result.Results...)
		log.Printf("filings: fetched page %d/%d (%d total so far)", page, result.Pagination.Pages, len(all))

		if page >= result.Pagination.Pages {
			break
		}
		page++
	}

	return all, nil
}

// --- Entry point ---

func main() {
	apiKey := os.Getenv("FEC_API_KEY")
	if apiKey == "" {
		log.Fatal("FEC_API_KEY environment variable is required")
	}

	ctx := context.Background()
	client := NewClient(apiKey)

	// Ingest candidates for the 2024 election cycle.
	log.Println("Ingesting candidates...")
	candidates, err := client.FetchCandidates(ctx, 2024)
	if err != nil {
		log.Fatalf("fetch candidates: %v", err)
	}
	log.Printf("Fetched %d candidates", len(candidates))

	// TODO: persist candidates to DuckDB (schema defined in store.go)
	// TODO: add more ingestion targets (filings, contributions, disbursements)
	// Example:
	//
	//	filings, err := client.FetchFilings(ctx, "C00.........")
}
