package sqlitezstd

import (
	"fmt"
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
		return nil, 0, sqlite3vfs.CantOpenError
	}

	decoder, err := zstd.NewReader(nil)
	if err != nil {
		return nil, 0, sqlite3vfs.CantOpenError
	}

	seekable, err := seekable.NewReader(file, decoder)
	if err != nil {
		return nil, 0, sqlite3vfs.CantOpenError
	}

	return &ZstdFile{
		decoder:  decoder,
		file:     file,
		seekable: seekable,
	}, flags | sqlite3vfs.OpenReadOnly, nil
}

func Init() error {
	err := sqlite3vfs.RegisterVFS("zstd", &ZstdVFS{})
	if err != nil {
		return fmt.Errorf("could not register vfs: %w", err)
	}

	return nil
}
