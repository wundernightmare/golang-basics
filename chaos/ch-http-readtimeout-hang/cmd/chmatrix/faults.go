package main

import (
	"bytes"
	"encoding/json"
	"fmt"
	"io"
	"net/http"
	"os/exec"
	"strings"
	"time"
)

const (
	toxiAPI          = "http://127.0.0.1:28474"
	chUpstream       = "chaoslab-ch:8123"
	chUpstreamNative = "chaoslab-ch:9000"
	chContainer      = "chaoslab-ch"
	nftTable         = "chaoslab"
)

var toxiHTTP = &http.Client{Timeout: 10 * time.Second}

func toxi(method, path string, body any) error {
	var rd io.Reader
	if body != nil {
		b, err := json.Marshal(body)
		if err != nil {
			return err
		}
		rd = bytes.NewReader(b)
	}
	req, err := http.NewRequest(method, toxiAPI+path, rd)
	if err != nil {
		return err
	}
	req.Header.Set("Content-Type", "application/json")
	resp, err := toxiHTTP.Do(req)
	if err != nil {
		return err
	}
	defer resp.Body.Close()
	out, _ := io.ReadAll(resp.Body)
	if resp.StatusCode >= 300 {
		return fmt.Errorf("toxiproxy %s %s: %s: %s", method, path, resp.Status, strings.TrimSpace(string(out)))
	}
	return nil
}

func proxyName(port int) string { return fmt.Sprintf("ch_%d", port) }

func createProxy(port int) error {
	_ = toxi(http.MethodDelete, "/proxies/"+proxyName(port), nil)
	upstream := chUpstream
	if proto == "native" {
		upstream = chUpstreamNative
	}
	return toxi(http.MethodPost, "/proxies", map[string]any{
		"name":     proxyName(port),
		"listen":   fmt.Sprintf("0.0.0.0:%d", port),
		"upstream": upstream,
		"enabled":  true,
	})
}

func deleteProxy(port int) { _ = toxi(http.MethodDelete, "/proxies/"+proxyName(port), nil) }

func addToxic(port int, name, typ, stream string, attrs map[string]any) error {
	return toxi(http.MethodPost, "/proxies/"+proxyName(port)+"/toxics", map[string]any{
		"name": name, "type": typ, "stream": stream, "toxicity": 1.0, "attributes": attrs,
	})
}

func delToxic(port int, name string) error {
	return toxi(http.MethodDelete, "/proxies/"+proxyName(port)+"/toxics/"+name, nil)
}

func run(name string, args ...string) error {
	out, err := exec.Command(name, args...).CombinedOutput()
	if err != nil {
		return fmt.Errorf("%s %s: %w: %s", name, strings.Join(args, " "), err, strings.TrimSpace(string(out)))
	}
	return nil
}

func nft(script string) error {
	cmd := exec.Command("sudo", "-n", "nft", "-f", "-")
	cmd.Stdin = strings.NewReader(script)
	out, err := cmd.CombinedOutput()
	if err != nil {
		return fmt.Errorf("nft %q: %w: %s", script, err, strings.TrimSpace(string(out)))
	}
	return nil
}

// nftDrop black-holes packets of the client<->proxy TCP connection on loopback:
// no RST, no FIN, no ICMP — the peers just stop hearing each other.
// toClient drops server->client segments, toServer drops client->server ones.
func nftDrop(port int, toServer, toClient bool) error {
	var sb strings.Builder
	fmt.Fprintf(&sb, "add table inet %s\n", nftTable)
	fmt.Fprintf(&sb, "add chain inet %s c%d { type filter hook output priority 0; }\n", nftTable, port)
	if toServer {
		fmt.Fprintf(&sb, "add rule inet %s c%d oifname lo tcp dport %d drop\n", nftTable, port, port)
	}
	if toClient {
		fmt.Fprintf(&sb, "add rule inet %s c%d oifname lo tcp sport %d drop\n", nftTable, port, port)
	}
	return nft(sb.String())
}

func nftHeal(port int) error {
	return nft(fmt.Sprintf("flush chain inet %s c%d\ndelete chain inet %s c%d\n", nftTable, port, nftTable, port))
}

type fault struct {
	desc   string
	inject func(port int) error
	heal   func(port int) error
}

var faults = map[string]fault{
	"none": {
		desc:   "no fault (control)",
		inject: func(int) error { return nil },
		heal:   func(int) error { return nil },
	},
	"toxi-timeout": {
		desc: "both directions go silent, TCP stays established (dead peer behind an LB / stuck middlebox)",
		inject: func(p int) error {
			if err := addToxic(p, "t_down", "timeout", "downstream", map[string]any{"timeout": 0}); err != nil {
				return err
			}
			return addToxic(p, "t_up", "timeout", "upstream", map[string]any{"timeout": 0})
		},
		heal: func(p int) error { _ = delToxic(p, "t_up"); return delToxic(p, "t_down") },
	},
	"toxi-timeout-down": {
		desc: "responses go silent, requests still delivered, TCP stays established",
		inject: func(p int) error {
			return addToxic(p, "t_down", "timeout", "downstream", map[string]any{"timeout": 0})
		},
		heal: func(p int) error { return delToxic(p, "t_down") },
	},
	"toxi-bandwidth": {
		desc: "link degraded to 1 KB/s in both directions (data keeps trickling)",
		inject: func(p int) error {
			if err := addToxic(p, "b_down", "bandwidth", "downstream", map[string]any{"rate": 1}); err != nil {
				return err
			}
			return addToxic(p, "b_up", "bandwidth", "upstream", map[string]any{"rate": 1})
		},
		heal: func(p int) error { _ = delToxic(p, "b_up"); return delToxic(p, "b_down") },
	},
	"toxi-reset": {
		desc: "connection reset by peer (RST)",
		inject: func(p int) error {
			if err := addToxic(p, "r_down", "reset_peer", "downstream", map[string]any{"timeout": 0}); err != nil {
				return err
			}
			return addToxic(p, "r_up", "reset_peer", "upstream", map[string]any{"timeout": 0})
		},
		heal: func(p int) error { _ = delToxic(p, "r_up"); return delToxic(p, "r_down") },
	},
	"toxi-down": {
		desc: "proxy disabled: established connections closed (FIN), new ones refused",
		inject: func(p int) error {
			return toxi(http.MethodPost, "/proxies/"+proxyName(p), map[string]any{"enabled": false})
		},
		heal: func(p int) error {
			return toxi(http.MethodPost, "/proxies/"+proxyName(p), map[string]any{"enabled": true})
		},
	},
	"nft-drop": {
		desc:   "packet black hole in both directions (nftables DROP: no RST/FIN/ICMP)",
		inject: func(p int) error { return nftDrop(p, true, true) },
		heal:   nftHeal,
	},
	"nft-drop-in": {
		desc:   "asymmetric black hole: server->client packets dropped, client->server delivered",
		inject: func(p int) error { return nftDrop(p, false, true) },
		heal:   nftHeal,
	},
	"ch-pause": {
		desc:   "ClickHouse process frozen (podman pause: kernel still ACKs, nobody answers)",
		inject: func(int) error { return run("podman", "pause", chContainer) },
		heal:   func(int) error { return run("podman", "unpause", chContainer) },
	},
}
