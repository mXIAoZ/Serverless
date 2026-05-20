package main

import (
	"encoding/json"
	"fmt"
	"io"
	"os"
	"time"
)

type event struct {
	Name    string `json:"name"`
	SleepMS int    `json:"sleep_ms"`
}

type response struct {
	StatusCode int    `json:"statusCode"`
	Message    string `json:"message"`
	RequestID  string `json:"requestId"`
}

func main() {
	payload, err := io.ReadAll(os.Stdin)
	if err != nil {
		fail(err)
	}

	var evt event
	if len(payload) > 0 {
		if err := json.Unmarshal(payload, &evt); err != nil {
			fail(err)
		}
	}

	if evt.SleepMS > 0 {
		time.Sleep(time.Duration(evt.SleepMS) * time.Millisecond)
	}
	if evt.Name == "" {
		evt.Name = "world"
	}

	resp := response{
		StatusCode: 200,
		Message:    fmt.Sprintf("Hello, %s!", evt.Name),
		RequestID:  os.Getenv("AWS_REQUEST_ID"),
	}

	if err := json.NewEncoder(os.Stdout).Encode(resp); err != nil {
		fail(err)
	}
}

func fail(err error) {
	fmt.Fprintln(os.Stderr, err)
	os.Exit(1)
}
