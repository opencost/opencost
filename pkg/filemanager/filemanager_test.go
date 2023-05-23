package filemanager

import (
	"context"
	"fmt"
	"io"
	"math/rand"
	"os"
	"path/filepath"
	"testing"

	"github.com/stretchr/testify/require"
)

func Test_NewFileManager(t *testing.T) {
	t.Run("file system", func(t *testing.T) {
		// Create File Manager
		tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("opencost-test-file-manager-%d", rand.Int31()))
		defer os.Remove(tmpPath) // remove managed file on completion to ensure it does not exist for next run
		fm, err := NewFileManager(tmpPath)
		require.NoError(t, err)

		// Attempt to download Managed file into a new file, this should fail because managed file has not been initalized
		downloadFile, err := os.CreateTemp("", "opencost-test-file-manager-*")
		require.NoError(t, err)
		err = fm.Download(context.TODO(), downloadFile)
		require.ErrorIs(t, err, ErrNotFound)

		// Create a file and add content to it
		uploadFile, err := os.CreateTemp("", "opencost-test-file-manager-*")
		require.NoError(t, err)
		_, err = uploadFile.WriteString("test-content")
		require.NoError(t, err)

		// Upload file to managed file
		err = fm.Upload(context.TODO(), uploadFile)
		require.NoError(t, err)

		// Download managed file to original file
		err = fm.Download(context.TODO(), downloadFile)
		require.NoError(t, err)

		// Check that content matches
		_, err = downloadFile.Seek(0, io.SeekStart)
		require.NoError(t, err)
		data, err := io.ReadAll(downloadFile)
		require.NoError(t, err)
		require.Equal(t, "test-content", string(data))
	})
}
