package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"net/url"
	"os"
	"sort"
	"strings"
	"time"

	"github.com/heath0xff/mrmr/internal/event"
)

// modelsResponse is the OpenAI-compatible /v1/models shape.
type modelsResponse struct {
	Object string       `json:"object"`
	Data   []modelEntry `json:"data"`
}

type modelEntry struct {
	ID      string `json:"id"`
	Object  string `json:"object"`
	Created int64  `json:"created"`
	OwnedBy string `json:"owned_by"`
}

// snapshot is the normalized endpoint state for diffing.
type snapshot struct {
	URL         string
	Name        string
	Reachable   bool
	ModelIDs    []string
	CheckedAt   time.Time
	ErrorDetail string
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("models-watcher", flag.ContinueOnError)
	mrmrURL := fs.String("mrmr", "http://localhost:4242/api/events", "mrmr ingest URL")
	endpointURL := fs.String("endpoint", "http://100.68.81.83:8999/v1/models", "OpenAI-compatible /v1/models endpoint to watch")
	// Scopes this instance's dedup namespace. Two watchers sharing one name
	// would collide at the RFC3339 second; defaults to the endpoint host.
	name := fs.String("name", "", "source name for this endpoint (default: endpoint host)")
	poll := fs.Duration("poll", 90*time.Second, "poll interval")
	if err := fs.Parse(args); err != nil {
		return err
	}

	// Default the source name to the endpoint host so two watchers are
	// distinct without extra config.
	if *name == "" {
		if u, err := url.Parse(*endpointURL); err == nil && u.Host != "" {
			*name = u.Host
		} else {
			*name = "models"
		}
	}

	client := &http.Client{Timeout: 10 * time.Second}
	var last *snapshot
	ticker := time.NewTicker(*poll)
	defer ticker.Stop()

	log.Printf("models-watcher started: name=%s endpoint=%s mrmr=%s poll=%s", *name, *endpointURL, *mrmrURL, poll)

	for {
		now := time.Now().UTC()
		cur, err := fetchSnapshot(*endpointURL)
		if err != nil {
			cur = &snapshot{Reachable: false, ErrorDetail: err.Error(), CheckedAt: now}
		} else {
			cur.CheckedAt = now
		}
		cur.URL = *endpointURL
		cur.Name = *name

		events := diffSnapshot(last, cur, now)
		for _, ev := range events {
			if err := postEvent(client, *mrmrURL, ev); err != nil {
				log.Printf("post event failed: id=%s err=%v", ev.ID, err)
				continue
			}
			log.Printf("sent: id=%s type=%s subject=%s", ev.ID, ev.Type, ev.Subject)
		}

		last = cur
		<-ticker.C
	}
}

// fetchSnapshot calls /v1/models and returns a normalized snapshot. It is
// intentionally read-only: no load/unload/adapter calls, only the model list.
func fetchSnapshot(url string) (*snapshot, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	req, err := http.NewRequestWithContext(ctx, http.MethodGet, url, nil)
	if err != nil {
		return nil, err
	}

	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		return nil, fmt.Errorf("endpoint unreachable: %w", err)
	}
	defer resp.Body.Close()

	if resp.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("endpoint returned %d", resp.StatusCode)
	}

	var models modelsResponse
	if err := json.NewDecoder(resp.Body).Decode(&models); err != nil {
		return nil, fmt.Errorf("decode /v1/models: %w", err)
	}

	ids := make([]string, 0, len(models.Data))
	for _, m := range models.Data {
		id := strings.TrimSpace(m.ID)
		if id != "" {
			ids = append(ids, id)
		}
	}
	sort.Strings(ids)

	return &snapshot{
		Reachable: true,
		ModelIDs:  ids,
	}, nil
}

// diffSnapshot emits events only for meaningful transitions.
// No-change intervals are silent and cost nothing.
func diffSnapshot(prev, cur *snapshot, now time.Time) []event.Event {
	if prev == nil {
		return initialEvents(cur, now)
	}

	var events []event.Event

	if !prev.Reachable && cur.Reachable {
		events = append(events, endpointEvent("inference.server.recovered", cur, now, "endpoint reachable", map[string]any{
			"loaded_models": cur.ModelIDs,
			"model_count":   len(cur.ModelIDs),
		}))
	}

	if prev.Reachable && !cur.Reachable {
		events = append(events, endpointEvent("inference.server.unreachable", cur, now, "endpoint unreachable", map[string]any{
			"error": cur.ErrorDetail,
		}))
	}

	if cur.Reachable {
		prevSet := set(prev.ModelIDs)
		curSet := set(cur.ModelIDs)

		for _, id := range cur.ModelIDs {
			if !prevSet[id] {
				events = append(events, endpointEvent("inference.model.loaded", cur, now, "model loaded", map[string]any{
					"model_id":      id,
					"loaded_models": cur.ModelIDs,
					"model_count":   len(cur.ModelIDs),
				}))
			}
		}
		for _, id := range prev.ModelIDs {
			if !curSet[id] {
				events = append(events, endpointEvent("inference.model.unloaded", cur, now, "model unloaded", map[string]any{
					"model_id":      id,
					"loaded_models": cur.ModelIDs,
					"model_count":   len(cur.ModelIDs),
				}))
			}
		}

		if len(prev.ModelIDs) != len(cur.ModelIDs) {
			events = append(events, endpointEvent("inference.models.changed", cur, now, "model count changed", map[string]any{
				"prev_count":    len(prev.ModelIDs),
				"model_count":   len(cur.ModelIDs),
				"loaded_models": cur.ModelIDs,
			}))
		}
	}

	return events
}

func initialEvents(cur *snapshot, now time.Time) []event.Event {
	if !cur.Reachable {
		return []event.Event{endpointEvent("inference.server.unreachable", cur, now, "endpoint unreachable on first check", map[string]any{
			"error": cur.ErrorDetail,
		})}
	}
	return []event.Event{endpointEvent("inference.server.recovered", cur, now, "endpoint reachable on first check", map[string]any{
		"loaded_models": cur.ModelIDs,
		"model_count":   len(cur.ModelIDs),
	})}
}

func set(ids []string) map[string]bool {
	m := make(map[string]bool, len(ids))
	for _, id := range ids {
		m[id] = true
	}
	return m
}

func endpointEvent(typ string, snap *snapshot, now time.Time, summary string, extra map[string]any) event.Event {
	data := map[string]any{
		"endpoint_url":  snap.URL,
		"reachable":     snap.Reachable,
		"loaded_models": snap.ModelIDs,
		"model_count":   len(snap.ModelIDs),
		"checked_at":    snap.CheckedAt.Format(time.RFC3339),
		"summary":       summary,
	}
	if !snap.Reachable && snap.ErrorDetail != "" {
		data["error"] = snap.ErrorDetail
	}
	for k, v := range extra {
		data[k] = v
	}
	return event.Event{
		ID:        event.NewID("evt_"),
		Type:      typ,
		Source:    snap.Name,
		Subject:   snap.URL,
		Timestamp: now,
		Data:      data,
		Metadata: map[string]any{
			"source_event_id": typ + ":" + snap.Name + ":" + now.Format(time.RFC3339),
		},
	}
}

func postEvent(client *http.Client, url string, ev event.Event) error {
	b, err := json.Marshal(ev)
	if err != nil {
		return err
	}
	req, err := http.NewRequestWithContext(context.Background(), http.MethodPost, url, bytes.NewReader(b))
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := client.Do(req)
	if err != nil {
		return err
	}
	resp.Body.Close()
	if resp.StatusCode >= 300 {
		return fmt.Errorf("unexpected status %d", resp.StatusCode)
	}
	return nil
}
