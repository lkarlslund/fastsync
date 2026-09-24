package main

import (
	"bytes"
	"encoding/json"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/rs/zerolog"
)

func TestLoggerWithFilePreservesDisplayAndAppendsJSON(t *testing.T) {
	path := filepath.Join(t.TempDir(), "events.jsonl")
	for _, message := range []string{"first", "second"} {
		file, err := openLogFile(path)
		if err != nil {
			t.Fatalf("open log file: %v", err)
		}
		var display bytes.Buffer
		logger := loggerWithFile(zerolog.InfoLevel, zerolog.ConsoleWriter{
			Out: &display, NoColor: true, TimeFormat: time.RFC3339,
		}, file)
		logger.Info().Str("name", "example.txt").Msg(message)
		logger.Debug().Msg("filtered")
		if err := file.Close(); err != nil {
			t.Fatalf("close log file: %v", err)
		}
		if !strings.Contains(display.String(), message) || strings.Contains(display.String(), "\x1b[") {
			t.Fatalf("display output = %q", display.String())
		}
	}
	info, err := os.Stat(path)
	if err != nil {
		t.Fatal(err)
	}
	if got := info.Mode().Perm(); got != 0600 {
		t.Fatalf("log permissions = %o, want 600", got)
	}
	data, err := os.ReadFile(path)
	if err != nil {
		t.Fatal(err)
	}
	lines := bytes.Split(bytes.TrimSpace(data), []byte{'\n'})
	if len(lines) != 2 {
		t.Fatalf("log lines = %d, want 2: %q", len(lines), data)
	}
	for i, line := range lines {
		var event map[string]any
		if err := json.Unmarshal(line, &event); err != nil {
			t.Fatalf("parse line %d: %v", i, err)
		}
		if got := event["message"]; got != []string{"first", "second"}[i] {
			t.Fatalf("line %d message = %v", i, got)
		}
	}
}

func TestOpenLogFileRejectsSymlink(t *testing.T) {
	dir := t.TempDir()
	target := filepath.Join(dir, "target")
	if err := os.WriteFile(target, []byte("existing"), 0600); err != nil {
		t.Fatal(err)
	}
	link := filepath.Join(dir, "link")
	if err := os.Symlink(target, link); err != nil {
		t.Fatal(err)
	}
	if file, err := openLogFile(link); err == nil {
		file.Close()
		t.Fatal("accepted symlink log path")
	}
	data, err := os.ReadFile(target)
	if err != nil || string(data) != "existing" {
		t.Fatalf("symlink target changed: %q, %v", data, err)
	}
}
