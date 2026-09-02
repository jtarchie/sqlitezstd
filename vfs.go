package sqlitezstd

import (
	"fmt"
	"io"
	"log/slog"
	"net/url"
	"os"
	"strings"
	"sync"

	seekable "github.com/SaveTheRbtz/zstd-seekable-format-go/pkg"
	"github.com/SaveTheRbtz/zstd-seekable-format-go/pkg/framecache"
	"github.com/klauspost/compress/zstd"
	_ "github.com/mattn/go-sqlite3"
	"github.com/psanford/httpreadat"
	"github.com/psanford/sqlite3vfs"
)

// ZstdVFS is a read-only sqlite3vfs.VFS for zstd-seekable compressed databases.
type ZstdVFS struct {
	opts *Options
}

var _ sqlite3vfs.VFS = &ZstdVFS{}

// sharedDecoder is a single, process-wide zstd decoder shared by every opened
// file. The seekable reader only ever calls DecodeAll, which is safe for
// concurrent use, so one decoder replaces the per-Open decoder pool that was
// allocated (and never closed) for each connection. It is intentionally never
// closed because it lives for the lifetime of the process.
//
// nolint: gochecknoglobals
var sharedDecoder = sync.OnceValues(func() (*zstd.Decoder, error) {
	return zstd.NewReader(nil)
})

// baseVFS is the VFS sqlite was already using -- the platform's own -- resolved
// once, lazily, since it is registered long before any database is opened. It
// is where Open sends the files this VFS cannot serve. Nil only if sqlite has
// no default VFS at all, which cannot happen in a linked-in build.
//
// nolint: gochecknoglobals
var baseVFS = sync.OnceValue(func() sqlite3vfs.VFS {
	return sqlite3vfs.VFSFind("")
})

// Register registers a zstd VFS under the given name with the supplied options.
// Open a database against it with the "?vfs=<name>" query parameter. The default
// "zstd" VFS (registered automatically on import) uses default options.
func Register(name string, opts ...Option) error {
	if err := sqlite3vfs.RegisterVFS(name, &ZstdVFS{opts: resolveOptions(opts)}); err != nil {
		return fmt.Errorf("could not register vfs %q: %w", name, err)
	}

	return nil
}

func isHTTP(name string) bool {
	return strings.HasPrefix(name, "http://") || strings.HasPrefix(name, "https://")
}

func (z *ZstdVFS) Access(name string, flags sqlite3vfs.AccessFlag) (bool, error) {
	if strings.HasSuffix(name, "-wal") || strings.HasSuffix(name, "-journal") {
		return false, nil
	}

	if isHTTP(name) {
		return true, nil
	}

	if _, err := os.Stat(name); err != nil {
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
	// Only the main database is a zstd archive. Everything else sqlite asks
	// this VFS for goes to the one underneath, which can create files -- above
	// all the temp file a sorter or a transient index spills into. Failing that
	// open used to fail the whole query with "unable to open database file",
	// and the only workaround was requiring PRAGMA temp_store = memory of every
	// caller. sqlite's own appendvfs diverts the same set.
	if flags&sqlite3vfs.OpenMainDB == 0 {
		if base := baseVFS(); base != nil {
			return base.Open(name, flags)
		}
	}

	file, err := z.open(name)
	if err != nil {
		// sqlite3vfs can only return fixed sentinel errors, so log the real
		// cause to make otherwise-identical "unable to open database" failures
		// diagnosable.
		z.opts.logger.Warn("sqlitezstd: failed to open database", "name", name, "error", err)

		return nil, 0, sqlite3vfs.CantOpenError
	}

	return file, flags | sqlite3vfs.OpenReadOnly, nil
}

// resolveSource opens the backing store for name (a local path or an HTTP(S)
// URL) and returns it as an io.ReaderAt together with its compressed size.
func (z *ZstdVFS) resolveSource(name string) (io.ReaderAt, int64, error) {
	if isHTTP(name) {
		uri, err := url.Parse(name)
		if err != nil {
			return nil, 0, fmt.Errorf("parse url: %w", err)
		}

		ranger := httpreadat.New(uri.String(), httpreadat.WithRoundTripper(newRangeRoundTripper(z.opts)))

		size, err := ranger.Size()
		if err != nil {
			return nil, 0, fmt.Errorf("determine remote size: %w", err)
		}

		return ranger, size, nil
	}

	// Opening the user-specified database path is the entire purpose of the VFS.
	osFile, err := os.Open(name) //nolint: gosec
	if err != nil {
		return nil, 0, fmt.Errorf("open file: %w", err)
	}

	info, err := osFile.Stat()
	if err != nil {
		_ = osFile.Close()

		return nil, 0, fmt.Errorf("stat file: %w", err)
	}

	return osFile, info.Size(), nil
}

// open does the real work and returns a descriptive error. Resources acquired
// along the way are released if a later step fails.
func (z *ZstdVFS) open(name string) (_ *ZstdFile, err error) {
	src, size, err := z.resolveSource(name)
	if err != nil {
		return nil, err
	}

	reader := &frameReader{
		src:  src,
		size: size,
	}

	defer func() {
		if err != nil {
			_ = reader.Close()
		}
	}()

	decoder, err := sharedDecoder()
	if err != nil {
		return nil, fmt.Errorf("create zstd decoder: %w", err)
	}

	sr, err := seekable.NewReader(
		reader,
		decoder,
		seekable.WithReaderFrameCache(framecache.NewSieve(framecache.Limits{
			MaxFrames: z.opts.frameCacheSize,
		})),
	)
	if err != nil {
		return nil, fmt.Errorf("create seekable reader: %w", err)
	}

	defer func() {
		if err != nil {
			_ = sr.Close()
		}
	}()

	// Capture the decompressed size once so FileSize() never has to call the
	// (non-goroutine-safe) Seek on the hot path.
	decompressedSize, err := sr.Seek(0, io.SeekEnd)
	if err != nil {
		return nil, fmt.Errorf("determine decompressed size: %w", err)
	}

	return &ZstdFile{
		reader:   reader,
		seekable: sr,
		size:     decompressedSize,
	}, nil
}

// once registers the default "zstd" VFS exactly once.
//
// nolint: gochecknoglobals
var once = sync.OnceValue(func() error {
	if err := sqlite3vfs.RegisterVFS("zstd", &ZstdVFS{opts: defaultOptions()}); err != nil {
		return fmt.Errorf("could not register vfs: %w", err)
	}

	return nil
})

// Deprecated: Init is a no-op retained for backward compatibility. The "zstd"
// VFS is registered automatically when the package is imported.
func Init() error {
	return nil
}

func init() {
	if err := once(); err != nil {
		// A library must not crash its host process; log instead of panicking.
		slog.Default().Error("sqlitezstd: could not register vfs", "error", err)
	}
}
