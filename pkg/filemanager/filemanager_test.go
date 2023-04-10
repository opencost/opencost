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
		tmpPath := filepath.Join(os.TempDir(), fmt.Sprintf("opencost-test-file-manager-%d", rand.Int31()))
		fm, err := NewFileManager(tmpPath)
		require.NoError(t, err)

		downloadFile, err := os.CreateTemp("", "opencost-test-file-manager-*")
		require.NoError(t, err)
		err = fm.Download(context.TODO(), downloadFile)
		require.ErrorIs(t, err, ErrNotFound)

		uploadFile, err := os.CreateTemp("", "opencost-test-file-manager-*")
		require.NoError(t, err)
		_, err = uploadFile.WriteString("test-content")
		require.NoError(t, err)
		require.NoError(t, err)
		err = fm.Upload(context.TODO(), uploadFile)
		require.NoError(t, err)

		err = fm.Download(context.TODO(), downloadFile)
		require.NoError(t, err)
		_, err = downloadFile.Seek(0, io.SeekStart)
		require.NoError(t, err)
		data, err := io.ReadAll(downloadFile)
		require.NoError(t, err)
		require.Equal(t, "test-content", string(data))
	})
}
