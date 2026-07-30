package azure

import (
	"errors"
	"os"
	"path/filepath"
	"strings"
	"testing"
	"time"

	"github.com/Azure/azure-sdk-for-go/sdk/storage/azblob/container"
)

// writeFile creates a file with the given contents and modification time and
// returns its os.FileInfo. Uses the real filesystem so the tests exercise real
// FileInfo values rather than a fake.
func writeFile(t *testing.T, path string, contents string, modTime time.Time) os.FileInfo {
	t.Helper()
	if err := os.WriteFile(path, []byte(contents), 0o600); err != nil {
		t.Fatalf("writing fixture file: %v", err)
	}
	if err := os.Chtimes(path, modTime, modTime); err != nil {
		t.Fatalf("setting fixture mtime: %v", err)
	}
	fi, err := os.Stat(path)
	if err != nil {
		t.Fatalf("stating fixture file: %v", err)
	}
	return fi
}

func testBlob(name string, lastModified *time.Time, contentLength *int64) container.BlobItem {
	return container.BlobItem{
		Name: &name,
		Properties: &container.BlobProperties{
			LastModified:  lastModified,
			ContentLength: contentLength,
		},
	}
}

func TestIsExistingFileCurrent(t *testing.T) {
	dir := t.TempDir()

	older := time.Date(2026, 5, 25, 12, 0, 0, 0, time.UTC)
	newer := time.Date(2026, 5, 26, 12, 0, 0, 0, time.UTC)

	const contents = "Date,Cost\n2026-05-25,1.00\n"
	size := int64(len(contents))

	testCases := map[string]struct {
		fileContents string
		fileModTime  time.Time
		blob         container.BlobItem
		expected     bool
		reason       string
	}{
		"blob newer than file is re-downloaded": {
			fileContents: contents,
			fileModTime:  older,
			blob:         testBlob("export.csv", &newer, &size),
			expected:     false,
			reason:       "the blob has been updated since the local copy was written",
		},
		"complete up-to-date file is reused": {
			fileContents: contents,
			fileModTime:  newer,
			blob:         testBlob("export.csv", &older, &size),
			expected:     true,
			reason:       "local copy is newer than the blob and its size matches",
		},
		// Regression: a download that fails partway (e.g. ENOSPC) or is killed
		// leaves a truncated file whose mtime is NEWER than the blob's, so an
		// mtime-only check treats the corrupt remnant as authoritative and the
		// bad data is parsed on every subsequent run.
		"truncated file with fresh mtime is re-downloaded": {
			fileContents: contents[:5],
			fileModTime:  newer,
			blob:         testBlob("export.csv", &older, &size),
			expected:     false,
			reason:       "on-disk size does not match the blob's ContentLength",
		},
		"zero length file with fresh mtime is re-downloaded": {
			fileContents: "",
			fileModTime:  newer,
			blob:         testBlob("export.csv", &older, &size),
			expected:     false,
			reason:       "os.Create truncated the file and the download never wrote any bytes",
		},
		"oversized file with fresh mtime is re-downloaded": {
			fileContents: contents + "2026-05-26,2.00\n",
			fileModTime:  newer,
			blob:         testBlob("export.csv", &older, &size),
			expected:     false,
			reason:       "on-disk size does not match the blob's ContentLength",
		},
		"missing ContentLength falls back to modification time": {
			fileContents: contents[:5],
			fileModTime:  newer,
			blob:         testBlob("export.csv", &older, nil),
			expected:     true,
			reason:       "size cannot be validated, so preserve the pre-existing mtime behaviour",
		},
		"missing LastModified is never treated as current": {
			fileContents: contents,
			fileModTime:  newer,
			blob:         testBlob("export.csv", nil, &size),
			expected:     false,
			reason:       "without a blob timestamp there is no basis to skip the download",
		},
		"nil Properties is never treated as current": {
			fileContents: contents,
			fileModTime:  newer,
			blob:         container.BlobItem{Name: ptr("export.csv")},
			expected:     false,
			reason:       "defensive: the SDK marks Properties required but it is a pointer",
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			path := filepath.Join(dir, strings.ReplaceAll(name, " ", "_"))
			fi := writeFile(t, path, tc.fileContents, tc.fileModTime)

			if got := isExistingFileCurrent(fi, tc.blob); got != tc.expected {
				t.Errorf("isExistingFileCurrent() = %v, want %v (%s)", got, tc.expected, tc.reason)
			}
		})
	}
}

func TestDownloadToFile(t *testing.T) {
	const contents = "Date,Cost\n2026-05-25,1.00\n"
	size := int64(len(contents))

	// tempFiles returns any leftover temp files in dir, i.e. files that are not
	// the destination. A correct implementation never leaves these behind.
	tempFiles := func(t *testing.T, dir, dest string) []string {
		t.Helper()
		entries, err := os.ReadDir(dir)
		if err != nil {
			t.Fatalf("reading dir: %v", err)
		}
		var leftovers []string
		for _, e := range entries {
			if filepath.Join(dir, e.Name()) != dest {
				leftovers = append(leftovers, e.Name())
			}
		}
		return leftovers
	}

	writeAll := func(f *os.File) (int64, error) {
		n, err := f.WriteString(contents)
		return int64(n), err
	}

	t.Run("successful download is renamed into place", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "export.csv")

		if _, err := downloadToFile(dest, &size, writeAll); err != nil {
			t.Fatalf("downloadToFile() error = %v, want nil", err)
		}

		got, err := os.ReadFile(dest)
		if err != nil {
			t.Fatalf("reading destination: %v", err)
		}
		if string(got) != contents {
			t.Errorf("destination contents = %q, want %q", got, contents)
		}
		if leftovers := tempFiles(t, dir, dest); len(leftovers) != 0 {
			t.Errorf("temp files left behind: %v", leftovers)
		}
	})

	t.Run("creates parent directories", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "db", "cloudcost", "export.csv")

		if _, err := downloadToFile(dest, &size, writeAll); err != nil {
			t.Fatalf("downloadToFile() error = %v, want nil", err)
		}
		if _, err := os.Stat(dest); err != nil {
			t.Errorf("destination not created: %v", err)
		}
	})

	// Regression: this is the ENOSPC case. The download fails partway; nothing
	// may be left at the destination path, because a partial file there would
	// be mistaken for a complete one on the next run.
	t.Run("failed download leaves no file at the destination", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "export.csv")
		wantErr := errors.New("no space left on device")

		_, err := downloadToFile(dest, &size, func(f *os.File) (int64, error) {
			if _, werr := f.WriteString(contents[:5]); werr != nil {
				return 0, werr
			}
			return 5, wantErr
		})
		if !errors.Is(err, wantErr) {
			t.Fatalf("downloadToFile() error = %v, want it to wrap %v", err, wantErr)
		}

		if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
			t.Errorf("destination exists after a failed download, stat err = %v", statErr)
		}
		if leftovers := tempFiles(t, dir, dest); len(leftovers) != 0 {
			t.Errorf("temp files left behind after failure: %v", leftovers)
		}
	})

	// Regression: os.Create truncated the destination before the download ran,
	// destroying a good local copy on any transient failure.
	t.Run("failed download preserves a pre-existing file", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "export.csv")
		const previous = "Date,Cost\n2026-05-24,9.99\n"
		if err := os.WriteFile(dest, []byte(previous), 0o600); err != nil {
			t.Fatalf("writing pre-existing file: %v", err)
		}

		_, err := downloadToFile(dest, &size, func(f *os.File) (int64, error) {
			return 0, errors.New("no space left on device")
		})
		if err == nil {
			t.Fatal("downloadToFile() error = nil, want an error")
		}

		got, readErr := os.ReadFile(dest)
		if readErr != nil {
			t.Fatalf("pre-existing file was destroyed: %v", readErr)
		}
		if string(got) != previous {
			t.Errorf("pre-existing contents = %q, want %q", got, previous)
		}
	})

	// A short read that the SDK does not report as an error still produces a
	// corrupt CSV, so the size is validated before the rename.
	t.Run("short download is rejected and not renamed into place", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "export.csv")

		_, err := downloadToFile(dest, &size, func(f *os.File) (int64, error) {
			n, werr := f.WriteString(contents[:5])
			return int64(n), werr
		})
		if err == nil {
			t.Fatal("downloadToFile() error = nil, want a size mismatch error")
		}
		if !strings.Contains(err.Error(), "size") {
			t.Errorf("error = %v, want it to mention the size mismatch", err)
		}
		if _, statErr := os.Stat(dest); !os.IsNotExist(statErr) {
			t.Errorf("short download was renamed into place, stat err = %v", statErr)
		}
		if leftovers := tempFiles(t, dir, dest); len(leftovers) != 0 {
			t.Errorf("temp files left behind after short download: %v", leftovers)
		}
	})

	t.Run("unknown expected size skips validation", func(t *testing.T) {
		dir := t.TempDir()
		dest := filepath.Join(dir, "export.csv")

		if _, err := downloadToFile(dest, nil, writeAll); err != nil {
			t.Fatalf("downloadToFile() error = %v, want nil", err)
		}
		if _, err := os.Stat(dest); err != nil {
			t.Errorf("destination not created: %v", err)
		}
	})
}
