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
	references := make(map[string]bool, len(c.Reference))
	for _, name := range c.Reference {
		references[name] = false
	}
	for _, entry := range entries {
		name := filepath.Base(entry.Name)
		_, reference := references[name]
		if reference {
			if !entry.IsDir {
				return nil, fmt.Errorf("reference %q is not a directory", name)
			}
			references[name] = true
		}
		included := false
		for i, pattern := range c.Include {
			matched, _ := path.Match(pattern, name) // validated before connecting
			if matched {
				matchedPatterns[i] = true
				included = true
			}
		}
		if included {
			if reference {
				return nil, fmt.Errorf("reference %q overlaps --include", name)
			}
			if !c.warmingReferences {
				selected = append(selected, entry)
			}
		} else if reference && c.warmingReferences {
			selected = append(selected, entry)
		}
	}
	for i, matched := range matchedPatterns {
		if !matched {
			return nil, fmt.Errorf("no source entry matches --include %q", c.Include[i])
		}
	}
	for name, matched := range references {
		if !matched {
			return nil, fmt.Errorf("no source directory matches --reference %q", name)
		}
	}
	return selected, nil
}

func (c *Client) isReference(name string) bool {
	first, _, _ := strings.Cut(relativeResumePath(name), string(filepath.Separator))
	for _, reference := range c.Reference {
		if first == reference {
			return true
		}
	}
	return false
}

func (c *Client) validateIncludes() error {
	if len(c.Reference) > 0 {
		if len(c.Include) == 0 || !c.PreserveHardlinks || c.Delete || c.ResumeCache != "" {
			return fmt.Errorf("--reference requires --include and hardlinks, and cannot use deletion or resume-cache")
		}
		for _, name := range c.Reference {
			if !filepath.IsLocal(name) || filepath.Base(name) != name || strings.ContainsAny(name, `\\/*?[]`) || name == "." {
				return fmt.Errorf("invalid --reference name %q: expected an exact top-level directory", name)
			}
		}
	}
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
