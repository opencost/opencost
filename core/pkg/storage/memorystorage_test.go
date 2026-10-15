package storage

import (
	"path"
	"testing"

	"github.com/opencost/opencost/core/pkg/util/json"
)

func TestMemoryStorage_List(t *testing.T) {
	store := NewMemoryStorage()
	testName := "list"

	fileNames := []string{
		"/file0.json",
		"/file1.json",
		"/dir0/file2.json",
		"/dir0/file3.json",
	}

	err := createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  []string
		expectErr bool
	}{
		"base dir files": {
			path: path.Join(testpath, testName),
			expected: []string{
				"file0.json",
				"file1.json",
			},
			expectErr: false,
		},
		"single nested dir files": {
			path: path.Join(testpath, testName, "dir0"),
			expected: []string{
				"file2.json",
				"file3.json",
			},
			expectErr: false,
		},
		"nonexistent dir files": {
			path:      path.Join(testpath, testName, "dir1"),
			expected:  []string{},
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			fileList, err := store.List(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if len(fileList) != len(tc.expected) {
				t.Errorf("file list length does not match expected length, actual: %d, expected: %d", len(fileList), len(tc.expected))
			}

			expectedSet := map[string]struct{}{}
			for _, expName := range tc.expected {
				expectedSet[expName] = struct{}{}
			}

			for _, file := range fileList {
				_, ok := expectedSet[file.Name]
				if !ok {
					t.Errorf("unexpect file in list %s", file.Name)
				}

				if file.Size == 0 {
					t.Errorf("file size is not set")
				}

				if file.ModTime.IsZero() {
					t.Errorf("file mod time is not set")
				}
			}
		})
	}
}

func TestMemoryStorage_ListDirectories(t *testing.T) {
	store := NewMemoryStorage()
	testName := "list_directories"

	fileNames := []string{
		"/file0.json",
		"/dir0/file2.json",
		"/dir0/file3.json",
		"/dir0/dir1/file4.json",
		"/dir0/dir2/file5.json",
	}

	err := createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  []string
		expectErr bool
	}{
		"base dir dir": {
			path: path.Join(testpath, testName),
			expected: []string{
				path.Join(testpath, testName, "dir0") + "/",
			},
			expectErr: false,
		},
		"single nested dir files": {
			path: path.Join(testpath, testName, "dir0"),
			expected: []string{
				path.Join(testpath, testName, "dir0", "dir1") + "/",
				path.Join(testpath, testName, "dir0", "dir2") + "/",
			},
			expectErr: false,
		},
		"dir with no sub dirs": {
			path:      path.Join(testpath, testName, "dir0/dir1"),
			expected:  []string{},
			expectErr: false,
		},
		"non-existent dir": {
			path:      path.Join(testpath, testName, "dir1"),
			expected:  []string{},
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			dirList, err := store.ListDirectories(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if len(dirList) != len(tc.expected) {
				t.Errorf("dir list length does not match expected length, actual: %d, expected: %d", len(dirList), len(tc.expected))
			}

			expectedSet := map[string]struct{}{}
			for _, expName := range tc.expected {
				expectedSet[expName] = struct{}{}
			}

			for _, dir := range dirList {
				_, ok := expectedSet[dir.Name]
				if !ok {
					t.Errorf("unexpect dir: %s in list %s", dir.Name, tc.path)
				}
			}
		})
	}
}

func TestMemoryStorage_Exists(t *testing.T) {
	store := NewMemoryStorage()
	testName := "exists"
	fileNames := []string{
		"/file0.json",
	}

	err := createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  bool
		expectErr bool
	}{
		"file exists": {
			path:      path.Join(testpath, testName, "file0.json"),
			expected:  true,
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expected:  false,
			expectErr: false,
		},
		"dir does not exist": {
			path:      path.Join(testpath, testName, "dir0/file.json"),
			expected:  false,
			expectErr: false,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			exists, err := store.Exists(tc.path)
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if exists != tc.expected {
				t.Errorf("file exists output did not match expected")
			}
		})
	}
}

func TestMemoryStorage_Read(t *testing.T) {
	store := NewMemoryStorage()
	testName := "read"

	fileNames := []string{
		"/file0.json",
	}

	err := createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expectErr bool
	}{
		"file exists": {
			path:      path.Join(testpath, testName, "file0.json"),
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expectErr: true,
		},
		"dir does not exist": {
			path:      path.Join(testpath, testName, "dir0/file.json"),
			expectErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			b, err := store.Read(tc.path)
			if tc.expectErr && err != nil {
				return
			}
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}
			var content testFileContent
			err = json.Unmarshal(b, &content)
			if err != nil {
				t.Errorf("could not unmarshal file content")
				return
			}

			if content != tfc {
				t.Errorf("file content did not match writen value")
			}
		})
	}
}

func TestMemoryStorage_Stat(t *testing.T) {
	store := NewMemoryStorage()
	testName := "stat"

	fileNames := []string{
		"/file0.json",
	}

	err := createFiles(fileNames, testName, store)
	if err != nil {
		t.Errorf("failed to create files: %s", err)
	}

	defer func() {
		err = cleanupFiles(fileNames, testName, store)
		if err != nil {
			t.Errorf("failed to clean up files: %s", err)
		}
	}()

	testCases := map[string]struct {
		path      string
		expected  *StorageInfo
		expectErr bool
	}{
		"base dir": {
			path: path.Join(testpath, testName, "file0.json"),
			expected: &StorageInfo{
				Name: "file0.json",
				Size: 45,
			},
			expectErr: false,
		},
		"file does not exist": {
			path:      path.Join(testpath, testName, "file1.json"),
			expected:  nil,
			expectErr: true,
		},
	}

	for name, tc := range testCases {
		t.Run(name, func(t *testing.T) {
			status, err := store.Stat(tc.path)
			if tc.expectErr && err != nil {
				return
			}
			if tc.expectErr == (err == nil) {
				if tc.expectErr {
					t.Errorf("expected error was not thrown")
					return
				}
				t.Errorf("unexpected error: %s", err.Error())
				return
			}

			if status.Name != tc.expected.Name {
				t.Errorf("status name did name match expected, actual: %s, expected: %s", status.Name, tc.expected.Name)
			}

			if status.Size != tc.expected.Size {
				t.Errorf("status name did size match expected, actual: %d, expected: %d", status.Size, tc.expected.Size)
			}

			if status.ModTime.IsZero() {
				t.Errorf("status mod time is not set")
			}

		})
	}
}

func TestMemoryStorage_ReadToLocalFile(t *testing.T) {
	store := NewMemoryStorage()
	TestStorageReadToLocalFile(t, store)
}

func TestMemoryStorage_ReadStream(t *testing.T) {
	store := NewMemoryStorage()
	TestStorageReadStream(t, store)
}

func TestMemoryStorage_WriteStream(t *testing.T) {
	store := NewMemoryStorage()
	TestStorageWriteStream(t, store)
}

// TestMemoryStorage_PathNormalization checks that a written file is reachable by every path that
// refers to it. Callers build storage paths with path.Join, so on Windows the written path never
// matched the normalized path the readers looked up, and every read of a written file failed.
func TestMemoryStorage_PathNormalization(t *testing.T) {
	testName := "path_normalization"
	canonical := path.Join(testpath, testName, "dir0/file0.json")

	// paths which all refer to the same file as canonical
	equivalent := []string{
		canonical,
		"./" + canonical,
		path.Join(testpath, testName) + "//dir0/file0.json",
		path.Join(testpath, testName, "dir1/../dir0/file0.json"),
	}

	for _, written := range equivalent {
		t.Run(written, func(t *testing.T) {
			store := NewMemoryStorage()
			if err := store.Write(written, []byte(written)); err != nil {
				t.Fatalf("failed to write file '%s': %s", written, err)
			}

			for _, p := range equivalent {
				b, err := store.Read(p)
				if err != nil {
					t.Errorf("failed to read '%s' after writing '%s': %s", p, written, err)
				} else if string(b) != written {
					t.Errorf("content of '%s' did not match written value: %s", p, string(b))
				}

				exists, err := store.Exists(p)
				if err != nil {
					t.Errorf("failed to check existence of '%s': %s", p, err)
				}
				if !exists {
					t.Errorf("'%s' does not exist after writing '%s'", p, written)
				}

				if _, err = store.Stat(p); err != nil {
					t.Errorf("failed to stat '%s' after writing '%s': %s", p, written, err)
				}
			}

			files, err := store.List(path.Join(testpath, testName, "dir0"))
			if err != nil {
				t.Fatalf("failed to list files: %s", err)
			}
			if len(files) != 1 {
				t.Fatalf("file list length does not match expected length, actual: %d, expected: %d", len(files), 1)
			}

			if err = store.Remove(canonical); err != nil {
				t.Fatalf("failed to remove file '%s': %s", canonical, err)
			}

			// removal must clear both the file tree and the direct path lookup
			exists, err := store.Exists(written)
			if err != nil {
				t.Fatalf("failed to check existence of '%s': %s", written, err)
			}
			if exists {
				t.Errorf("'%s' still exists after removing '%s'", written, canonical)
			}

			files, err = store.List(path.Join(testpath, testName, "dir0"))
			if err != nil {
				t.Fatalf("failed to list files: %s", err)
			}
			if len(files) != 0 {
				t.Errorf("file list length does not match expected length, actual: %d, expected: %d", len(files), 0)
			}
		})
	}
}
