package main

import (
	"fmt"
	"io"
	"strings"

	"github.com/lkarlslund/fastsync"
)

// A file avoids exposing credentials in process arguments or shell history.
// Ignore one text-file line ending, but preserve intentional spaces.
func readPasswordFile(path string) (string, error) {
	if path == "" {
		return "", nil
	}
	f, err := fastsync.OpenFileNoFollow(path, 0, 0)
	if err != nil {
		return "", fmt.Errorf("open password file: %w", err)
	}
	defer f.Close()
	info, err := f.Stat()
	if err != nil {
		return "", err
	}
	if !info.Mode().IsRegular() || info.Mode().Perm()&0077 != 0 {
		return "", fmt.Errorf("password file must be a regular file accessible only to its owner (chmod 600)")
	}
	data, err := io.ReadAll(io.LimitReader(f, 4097))
	if err != nil {
		return "", err
	}
	if len(data) > 4096 {
		return "", fmt.Errorf("password file exceeds 4096 bytes")
	}
	password := strings.TrimSuffix(strings.TrimSuffix(string(data), "\n"), "\r")
	if password == "" || strings.ContainsAny(password, "\r\n\x00") {
		return "", fmt.Errorf("password file must contain one nonempty line")
	}
	return password, nil
}
