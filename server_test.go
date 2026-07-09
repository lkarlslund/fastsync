package fastsync

import (
	"errors"
	"path/filepath"
	"testing"
)

func TestServerLocalPath(t *testing.T) {
	base := t.TempDir()
	s := &Server{BasePath: base}

	tests := []struct {
		name    string
		path    string
		want    string
		wantErr error
	}{
		{name: "root slash", path: "/", want: base},
		{name: "root dot", path: ".", want: base},
		{name: "relative", path: "dir/file", want: filepath.Join(base, "dir", "file")},
		{name: "protocol absolute", path: "/dir/file", want: filepath.Join(base, "dir", "file")},
		{name: "parent escape", path: "../secret", wantErr: ErrInvalidPath},
		{name: "nested parent escape", path: "dir/../../secret", wantErr: ErrInvalidPath},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got, err := s.localPath(tt.path)
			if tt.wantErr != nil {
				if !errors.Is(err, tt.wantErr) {
					t.Fatalf("localPath(%q) error = %v, want %v", tt.path, err, tt.wantErr)
				}
				return
			}
			if err != nil {
				t.Fatalf("localPath(%q) unexpected error: %v", tt.path, err)
			}
			if got != tt.want {
				t.Fatalf("localPath(%q) = %q, want %q", tt.path, got, tt.want)
			}
		})
	}
}
