package main

import (
	"bytes"
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/exec"
	"runtime"
	"strings"
	"time"

	"github.com/heath0xff/mrmr/internal/event"
)

// peerSnapshot captures one Tailscale peer for state-change detection.
type peerSnapshot struct {
	HostName string `json:"hostname"`
	DNSName  string `json:"dns_name"`
	Online   bool   `json:"online"`
	Relay    string `json:"relay"`
	OS       string `json:"os"`
	TailIP   string `json:"tailscale_ip"`
	LastSeen string `json:"last_seen"`
}

// tailscaleStatus is the normalized view of `tailscale status --json`.
type tailscaleStatus struct {
	Self  peerSnapshot            `json:"self"`
	Peers map[string]peerSnapshot `json:"peers"`
}

func main() {
	if err := run(os.Args[1:]); err != nil {
		log.Fatal(err)
	}
}

func run(args []string) error {
	fs := flag.NewFlagSet("tailscale-watcher", flag.ContinueOnError)
	mrmrURL := fs.String("mrmr", "http://localhost:4242/api/events", "mrmr ingest URL")
	poll := fs.Duration("poll", 30*time.Second, "poll interval")
	selfHost := fs.String("self-host", "", "self hostname label; defaults to current hostname")
	exclude := fs.String("exclude", "", "comma-separated peer hostnames to ignore")
	if err := fs.Parse(args); err != nil {
		return err
	}

	excludes := make(map[string]bool)
	for _, h := range strings.Split(*exclude, ",") {
		h = strings.TrimSpace(h)
		if h != "" {
			excludes[strings.ToLower(h)] = true
		}
	}

	self := *selfHost
	if self == "" {
		self, _ = os.Hostname()
	}

	client := &http.Client{Timeout: 10 * time.Second}

	var last *tailscaleStatus
	ticker := time.NewTicker(*poll)
	defer ticker.Stop()

	if _, err := exec.LookPath("tailscale"); err != nil {
		return fmt.Errorf("tailscale not found on PATH: %w", err)
	}

	log.Printf("tailscale-watcher started: target=%s poll=%s self=%s", *mrmrURL, poll, self)

	for {
		now := time.Now().UTC()
		status, err := fetchStatus()
		if err != nil {
			log.Printf("fetch tailscale status failed: %v", err)
			time.Sleep(*poll)
			continue
		}

		events := diffPeers(last, status, self, excludes, now)
		for _, ev := range events {
			if err := postEvent(client, *mrmrURL, ev); err != nil {
				log.Printf("post event failed: id=%s err=%v", ev.ID, err)
				continue
			}
			log.Printf("sent: id=%s type=%s subject=%s", ev.ID, ev.Type, ev.Subject)
		}

		last = status
		<-ticker.C
	}
}

// fetchStatus shells out once per tick and decodes `tailscale status --json`.
// It owns the CLI invocation and the raw JSON shape; everything else is in
// normal event space.
func fetchStatus() (*tailscaleStatus, error) {
	ctx, cancel := context.WithTimeout(context.Background(), 8*time.Second)
	defer cancel()

	cmd := exec.CommandContext(ctx, "tailscale", "status", "--json")
	var out bytes.Buffer
	cmd.Stdout = &out
	cmd.Stderr = &out

	if runtime.GOOS == "darwin" {
		cmd.Env = append(os.Environ(), "PATH", "/usr/local/bin:/opt/homebrew/bin:"+os.Getenv("PATH"))
	}
	if err := cmd.Run(); err != nil {
		if ctx.Err() != nil {
			return nil, fmt.Errorf("tailscale status timed out: %w", err)
		}
		return nil, fmt.Errorf("tailscale status failed: %w: %s", err, strings.TrimSpace(out.String()))
	}

	var raw struct {
		Self   map[string]any            `json:"Self"`
		Peer   map[string]map[string]any `json:"Peer"`
		Health []any                     `json:"Health"`
	}
	if err := json.Unmarshal(out.Bytes(), &raw); err != nil {
		return nil, fmt.Errorf("decode tailscale json: %w", err)
	}

	status := &tailscaleStatus{Peers: make(map[string]peerSnapshot, len(raw.Peer))}
	if s, ok := decodePeer(raw.Self); ok {
		status.Self = s
	}
	for k, v := range raw.Peer {
		if s, ok := decodePeer(v); ok {
			status.Peers[k] = s
		}
	}
	_ = raw.Health

	return status, nil
}

func decodePeer(raw map[string]any) (peerSnapshot, bool) {
	var s peerSnapshot
	hname, _ := raw["HostName"].(string)
	if hname == "" {
		return s, false
	}
	s.HostName = hname
	s.DNSName, _ = raw["DNSName"].(string)
	s.OS, _ = raw["OS"].(string)
	s.Relay, _ = raw["Relay"].(string)
	s.TailIP = firstIP(raw["TailscaleIPs"])
	s.LastSeen, _ = raw["LastSeen"].(string)
	s.Online, _ = raw["Online"].(bool)
	return s, true
}

func firstIP(v any) string {
	arr, ok := v.([]any)
	if !ok || len(arr) == 0 {
		return ""
	}
	s, _ := arr[0].(string)
	return s
}

// diffPeers produces events only for meaningful transitions on watched peers:
// new appearance, loss of online state, relay changes, and key expiry risks.
// Re-emit only when state actually changed; stale snapshots emit nothing.
func diffPeers(prev, cur *tailscaleStatus, self string, excludes map[string]bool, now time.Time) []event.Event {
	if prev == nil {
		return initialEvents(cur, self, excludes, now)
	}
	var events []event.Event
	for key, curPeer := range cur.Peers {
		if excludePeer(curPeer, excludes) {
			continue
		}
		prevPeer, ok := prev.Peers[key]
		if !ok {
			events = append(events, peerEvent(curPeer, "tailscale.peer.discovered", self, now, "peer first seen by watcher", nil))
			continue
		}
		if curPeer.Online != prevPeer.Online || curPeer.Relay != prevPeer.Relay || curPeer.TailIP != prevPeer.TailIP {
			kind := "recovered"
			summary := "peer returned online"
			if !curPeer.Online {
				kind = "went offline"
				summary = "peer is offline"
			} else if curPeer.Relay != prevPeer.Relay {
				summary = "peer path changed"
			}
			events = append(events, peerEvent(curPeer, "tailscale.peer."+kind, self, now, summary, map[string]any{
				"prev_online": prevPeer.Online,
				"prev_relay":  prevPeer.Relay,
				"prev_ip":     prevPeer.TailIP,
			}))
		}
	}
	for key, prevPeer := range prev.Peers {
		if excludePeer(prevPeer, excludes) {
			continue
		}
		if _, ok := cur.Peers[key]; !ok {
			events = append(events, peerEvent(prevPeer, "tailscale.peer.lost", self, now, "peer disappeared from tailnet", map[string]any{
				"last_online": prevPeer.Online,
				"last_relay":  prevPeer.Relay,
				"last_ip":     prevPeer.TailIP,
				"last_seen":   prevPeer.LastSeen,
			}))
		}
	}
	return events
}

func initialEvents(status *tailscaleStatus, self string, excludes map[string]bool, now time.Time) []event.Event {
	var events []event.Event
	for _, p := range status.Peers {
		if excludePeer(p, excludes) {
			continue
		}
		summary := "peer is offline"
		if p.Online {
			summary = "peer is online"
		}
		events = append(events, peerEvent(p, "tailscale.peer.discovered", self, now, summary, map[string]any{
			"relay": p.Relay,
			"os":    p.OS,
		}))
	}
	return events
}

func excludePeer(p peerSnapshot, excludes map[string]bool) bool {
	if len(excludes) == 0 {
		return false
	}
	return excludes[strings.ToLower(p.HostName)]
}

func peerEvent(p peerSnapshot, typ, self string, now time.Time, summary string, extra map[string]any) event.Event {
	data := map[string]any{
		"hostname":     p.HostName,
		"dns_name":     p.DNSName,
		"online":       p.Online,
		"relay":        p.Relay,
		"os":           p.OS,
		"tailscale_ip": p.TailIP,
		"last_seen":    p.LastSeen,
		"summary":      summary,
	}
	for k, v := range extra {
		data[k] = v
	}
	return event.Event{
		ID:        event.NewID("evt_"),
		Type:      typ,
		Source:    "tailscale",
		Subject:   self + " -> " + p.HostName,
		Timestamp: now,
		Data:      data,
		Metadata: map[string]any{
			"source_event_id": typ + ":" + p.HostName + ":" + now.Format(time.RFC3339),
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
