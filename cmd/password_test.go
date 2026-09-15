package main

import (
	"os"
	"path/filepath"
	"testing"
)

func TestReadPasswordFile(t *testing.T) {
	if p, e := readPasswordFile(""); p != "" || e != nil {
		t.Fatal(p, e)
	}
	for _, tc := range []struct {
		content string
		mode    os.FileMode
		valid   bool
	}{
		{"synthetic credential\n", 0600, true}, {" spaces preserved \r\n", 0400, true},
		{"", 0600, false}, {"\n", 0600, false}, {"line\nsecond", 0600, false}, {"synthetic", 0644, false},
	} {
		p := filepath.Join(t.TempDir(), "credential")
		if err := os.WriteFile(p, []byte(tc.content), tc.mode); err != nil {
			t.Fatal(err)
		}
		got, err := readPasswordFile(p)
		if (err == nil) != tc.valid {
			t.Fatalf("mode %o valid=%v err=%v", tc.mode, tc.valid, err)
		}
		if tc.valid && got == "" {
			t.Fatal("lost credential")
		}
	}
}
