package main

import (
	"reflect"
	"testing"
)

func TestBootstrapCommand(t *testing.T) {
	tests := []struct {
		name    string
		runtime string
		want    []string
	}{
		{name: "empty defaults to python3", runtime: "", want: []string{"python3", "/runtime/bootstrap/python3_bootstrap.py"}},
		{name: "python3", runtime: "python3", want: []string{"python3", "/runtime/bootstrap/python3_bootstrap.py"}},
		{name: "go", runtime: "go", want: []string{"/runtime/bootstrap/go-bootstrap"}},
		{name: "nodejs", runtime: "nodejs", want: []string{"node", "/runtime/bootstrap/nodejs_bootstrap.js"}},
		{name: "java", runtime: "java", want: []string{"java", "-cp", "/runtime/bootstrap/java-bootstrap.jar", "JavaBootstrap"}},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := bootstrapCommand(tt.runtime)
			if err != nil {
				t.Fatal(err)
			}
			if !reflect.DeepEqual(got, tt.want) {
				t.Fatalf("bootstrapCommand(%q) = %#v, want %#v", tt.runtime, got, tt.want)
			}
		})
	}
}

func TestBootstrapCommandRejectsUnsupportedRuntime(t *testing.T) {
	if _, err := bootstrapCommand("ruby"); err == nil {
		t.Fatal("expected unsupported runtime to fail")
	}
}
