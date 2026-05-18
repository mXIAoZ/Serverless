package main

import (
	"bufio"
	"context"
	"encoding/binary"
	"encoding/json"
	"io"
	"log"
	"net"
	"net/http"
	"net/url"
	"strings"
	"time"
)

func dockerDo(method, path string, body io.Reader) (*http.Response, error) {
	transport := &http.Transport{
		DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
			return (&net.Dialer{}).DialContext(ctx, "unix", dockerSock)
		},
	}
	client := &http.Client{Transport: transport}
	req, err := http.NewRequest(method, "http://docker"+path, body)
	if err != nil {
		return nil, err
	}
	return client.Do(req)
}

func (d *daemon) watchEvents(ctx context.Context) {
	filters := url.QueryEscape(`{"event":["start","die"],"label":["` + labelKey + `"]}`)
	for {
		resp, err := dockerDo("GET", "/events?filters="+filters, nil)
		if err != nil {
			log.Printf("[daemon] events error: %v — retrying in 3s", err)
			time.Sleep(3 * time.Second)
			continue
		}

		scanner := bufio.NewScanner(resp.Body)
		for scanner.Scan() {
			var ev struct {
				Status string `json:"status"`
				ID     string `json:"id"`
				Actor  struct {
					Attributes map[string]string `json:"Attributes"`
				} `json:"Actor"`
			}
			if err := json.Unmarshal(scanner.Bytes(), &ev); err != nil {
				continue
			}
			funcName := ev.Actor.Attributes[labelKey]
			if funcName == "" {
				continue
			}
			switch ev.Status {
			case "start":
				log.Printf("[daemon] container started: %s (%s)", ev.ID[:12], funcName)
				go d.collectLogs(ctx, ev.ID, funcName)
			case "die":
				log.Printf("[daemon] container stopped: %s (%s)", ev.ID[:12], funcName)
			}
		}
		resp.Body.Close()

		select {
		case <-ctx.Done():
			return
		default:
			time.Sleep(time.Second)
		}
	}
}

func (d *daemon) collectLogs(ctx context.Context, containerID, funcName string) {
	path := "/containers/" + containerID + "/logs?follow=1&stdout=1&stderr=1&timestamps=1"
	resp, err := dockerDo("GET", path, nil)
	if err != nil {
		log.Printf("[daemon] logs attach %s: %v", containerID[:12], err)
		return
	}
	defer resp.Body.Close()

	hdr := make([]byte, 8)
	for {
		if _, err := io.ReadFull(resp.Body, hdr); err != nil {
			if err != io.EOF && err != io.ErrUnexpectedEOF {
				log.Printf("[daemon] log stream %s ended: %v", containerID[:12], err)
			}
			return
		}
		streamType := hdr[0]
		size := binary.BigEndian.Uint32(hdr[4:])
		payload := make([]byte, size)
		if _, err := io.ReadFull(resp.Body, payload); err != nil {
			return
		}

		stream := "stdout"
		if streamType == 2 {
			stream = "stderr"
		}
		line := strings.TrimRight(string(payload), "\n")
		ts := time.Now()
		if idx := strings.Index(line, " "); idx > 0 {
			if t, err := time.Parse(time.RFC3339Nano, line[:idx]); err == nil {
				ts = t
				line = line[idx+1:]
			}
		}
		d.write(LogEntry{Time: ts, Function: funcName, Stream: stream, Line: line})
	}
}

func (d *daemon) collectExisting(ctx context.Context) {
	filters := url.QueryEscape(`{"label":["` + labelKey + `"],"status":["running"]}`)
	resp, err := dockerDo("GET", "/containers/json?filters="+filters, nil)
	if err != nil {
		log.Printf("[daemon] list existing containers: %v", err)
		return
	}
	defer resp.Body.Close()

	var containers []struct {
		ID     string            `json:"Id"`
		Labels map[string]string `json:"Labels"`
	}
	if err := json.NewDecoder(resp.Body).Decode(&containers); err != nil {
		return
	}
	for _, c := range containers {
		funcName := c.Labels[labelKey]
		if funcName == "" {
			continue
		}
		log.Printf("[daemon] attaching to existing container %s (%s)", c.ID[:12], funcName)
		go d.collectLogs(ctx, c.ID, funcName)
	}
}
