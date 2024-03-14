package sqlitezstd

import (
	"fmt"
	"io"
	"os"
	"strings"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/sqlite3vfs"
)

type ZstdVFS struct{}

var _ sqlite3vfs.VFS = &ZstdVFS{}

func (z *ZstdVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-journal") {
		return false, nil
	}

	return true, nil
}

func (z *ZstdVFS) Delete(name string, dirSync bool) error {
	return sqlite3vfs.ReadOnlyError
}

func (z *ZstdVFS) FullPathname(name string) string {
	return name
}

func (z *ZstdVFS) Open(name string, flags sqlite3vfs.OpenFlag) (sqlite3vfs.File, sqlite3vfs.OpenFlag, error) {
	file, err := os.Open(name)
	if err != nil {
		return nil, 0, fmt.Errorf("could not open file: %w", err)
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, 0, fmt.Errorf("could not create reader: %w", err)
	}

	seekable, err := seekable.NewReader(file, decoder)
	if err != nil {
		return nil, 0, fmt.Errorf("could not create seekable: %w", err)
	}

	size, err := seekable.Seek(-1, io.SeekEnd)
	if err != nil {
		return nil, 0, fmt.Errorf("could not find size of db file: %w", err)
	}

	_, _ = seekable.Seek(0, io.SeekStart)

	return &ZstdFile{
		decoder:  decoder,
		file:     file,
		seekable: seekable,
		size:     size,
	}, 0, nil
}

func Init() error {
	err := sqlite3vfs.RegisterVFS("zstd", &ZstdVFS{})
	if err != nil {
		return fmt.Errorf("could not register vfs: %w", err)
	}

	return nil
}
