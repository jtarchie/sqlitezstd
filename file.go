package sqlitezstd

import (
	"io"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go"
	"github.com/klauspost/compress/zstd"
	"github.com/psanford/sqlite3vfs"
)

type ZstdFile struct {
	decoder  *zstd.Decoder
	reader   io.ReadSeeker
	seekable seekable.Reader
}

var _ sqlite3vfs.File = &ZstdFile{}

func (z *ZstdFile) CheckReservedLock() (bool, error) {
	return false, nil
}

func (z *ZstdFile) Close() error {
	_ = z.seekable.Close()

	if closer, ok := z.reader.(io.Closer); ok {
		_ = closer.Close()
	}

	return nil
}

func (z *ZstdFile) DeviceCharacteristics() sqlite3vfs.DeviceCharacteristic {
	return sqlite3vfs.IocapImmutable
}

func (z *ZstdFile) FileSize() (int64, error) {
	return z.seekable.Seek(0, io.SeekEnd)
}

func (z *ZstdFile) Lock(elock sqlite3vfs.LockType) error {
	return nil
}

func (z *ZstdFile) ReadAt(p []byte, off int64) (int, error) {
	return z.seekable.ReadAt(p, off)
}

func (z *ZstdFile) SectorSize() int64 {
	return 0
}

func (z *ZstdFile) Sync(flag sqlite3vfs.SyncType) error {
	return nil
}

func (z *ZstdFile) Truncate(size int64) error {
	return sqlite3vfs.ReadOnlyError
}

func (z *ZstdFile) Unlock(elock sqlite3vfs.LockType) error {
	return nil
}

func (z *ZstdFile) WriteAt(p []byte, off int64) (int, error) {
	return 0, sqlite3vfs.ReadOnlyError
}
