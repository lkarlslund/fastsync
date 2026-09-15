package main

import (
	"bytes"
	"os"
	"path/filepath"
	"strings"
	"testing"
)

func TestVerifyReportProtection(t *testing.T) {
	previous := directory
	directory = t.TempDir()
	defer func() { directory = previous }()
	for _, tc := range []struct{ name, path, want string }{
		{"inside archive", filepath.Join(directory, "report.jsonl"), "outside the archive"},
		{"existing report", filepath.Join(t.TempDir(), "report.jsonl"), "file exists"},
	} {
		t.Run(tc.name, func(t *testing.T) {
			if tc.name == "existing report" {
				if err := os.WriteFile(tc.path, []byte("original"), 0600); err != nil {
					t.Fatal(err)
				}
			}
			command := newVerifyCommand()
			command.SetOut(&bytes.Buffer{})
			command.SetErr(&bytes.Buffer{})
			command.SetArgs([]string{"127.0.0.1:1", "--report", tc.path})
			if err := command.Execute(); err == nil || !strings.Contains(err.Error(), tc.want) {
				t.Fatalf("got %v, want %s", err, tc.want)
			}
			if tc.name == "existing report" {
				data, err := os.ReadFile(tc.path)
				if err != nil || string(data) != "original" {
					t.Fatal("report overwritten")
				}
			}
		})
	}
}
func TestMemoryMeasurement(t *testing.T) {
	used, err := processMemory()
	if err != nil || used == 0 {
		t.Fatalf("process memory = %d, %v", used, err)
	}
}
