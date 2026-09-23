package fastsync

import (
	"fmt"
	"path"
	"path/filepath"
	"strings"
)

// Include patterns match direct children of the selected source root. A matched
// directory is traversed in full, including all its descendants.
func (c *Client) selectTopLevel(entries []FileInfo) ([]FileInfo, error) {
	if len(c.Include) == 0 {
		return entries, nil
	}
	selected := make([]FileInfo, 0, len(entries))
	matchedPatterns := make([]bool, len(c.Include))
	for _, entry := range entries {
		name := filepath.Base(entry.Name)
		included := false
		for i, pattern := range c.Include {
			matched, _ := path.Match(pattern, name) // validated before connecting
			if matched {
				matchedPatterns[i] = true
				included = true
			}
		}
		if included {
			selected = append(selected, entry)
		}
	}
	for i, matched := range matchedPatterns {
		if !matched {
			return nil, fmt.Errorf("no source entry matches --include %q", c.Include[i])
		}
	}
	return selected, nil
}

func (c *Client) validateIncludes() error {
	if len(c.Include) > 0 && c.Delete {
		return fmt.Errorf("--include cannot be combined with --delete")
	}
	for _, pattern := range c.Include {
		if pattern == "" || pattern == "." || pattern == ".." || strings.ContainsAny(pattern, `/\`) {
			return fmt.Errorf("invalid --include pattern %q: expected a single top-level name or glob", pattern)
		}
		if _, err := path.Match(pattern, ""); err != nil {
			return fmt.Errorf("invalid --include pattern %q: %w", pattern, err)
		}
	}
	return nil
}
