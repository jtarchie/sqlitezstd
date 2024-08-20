# SQLiteZSTD: Read-Only Access to Compressed SQLite Files

> [!IMPORTANT] A new version of this extension written in C is now available.
> This C version offers the advantage of being usable across different
> platforms, languages, and runtimes. It is not publicly available and is
> provided under a one-time fee in perpetuity license with support. The original
> Go version will remain freely available. For more information about the C
> extension, please email jtarchie@gmail.com.

## Description

SQLiteZSTD provides a tool for accessing SQLite databases compressed with
[Zstandard seekable (zstd)](https://github.com/facebook/zstd/blob/216099a73f6ec19c246019df12a2877dada45cca/contrib/seekable_format/zstd_seekable_compression_format.md)
in a read-only manner. Its functionality is based on the
[SQLite3 Virtual File System (VFS) in Go](https://github.com/psanford/sqlite3vfs).

Please note, SQLiteZSTD is specifically designed for reading data and **does not
support write operations**.

## Features

1. Read-only access to Zstd-compressed SQLite databases.
2. Interface through SQLite3 VFS.
3. The compressed database is seekable, facilitating ease of access.

## Usage

Your database needs to be compressed in the seekable Zstd format. I recommend
using this [CLI tool](github.com/SaveTheRbtz/zstd-seekable-format-go):

```bash
go get -a github.com/SaveTheRbtz/zstd-seekable-format-go/...
go run github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek \
    -f <dbPath> \
    -o <dbPath>.zst
```

The CLI provides different options for compression levels, but I do not have
specific recommendations for best usage patterns.

Below is an example of how to use SQLiteZSTD in a Go program:

```go
import (
    sqlitezstd "github.com/jtarchie/sqlitezstd"
)

initErr := sqlitezstd.Init()
if initErr != nil {
    panic(fmt.Sprintf("Failed to initialize SQLiteZSTD: %s", initErr))
}

db, err := sql.Open("sqlite3", "<path-to-your-file>?vfs=zstd")
if err != nil {
    panic(fmt.Sprintf("Failed to open database: %s", err))
}

_, err = db.Exec(`PRAGMA temp_store = memory;`)
if err != nil {
    panic(fmt.Sprintf("Failed to set PRAGMA: %s", err))
}
```

In this Go code example:

- The SQLiteZSTD library is initialized first with `sqlitezstd.Init()`.
- The `sql.Open()` function takes as a parameter the path to the compressed
  SQLite database, appended with a query string with `vfs=zstd` to use the VFS.
- Setting the `PRAGMA` ensures that the read only VFS is not used to create
  temporary files.

## Performance

Here's a simple benchmark comparing performance between reading from an
uncompressed vs. a compressed SQLite database, involving the insertion of 10k
records and retrieval of the `MAX` value (without an index) and FTS5.

```
BenchmarkReadUncompressedSQLite-4              	  159717	      7459 ns/op	     473 B/op	      15 allocs/op
BenchmarkReadUncompressedSQLiteFTS5Porter-4    	    2478	    471685 ns/op	     450 B/op	      15 allocs/op
BenchmarkReadUncompressedSQLiteFTS5Trigram-4   	     100	  10449792 ns/op	     542 B/op	      16 allocs/op
BenchmarkReadCompressedSQLite-4                	  266703	      3877 ns/op	    2635 B/op	      15 allocs/op
BenchmarkReadCompressedSQLiteFTS5Porter-4      	    2335	    487430 ns/op	   33992 B/op	      16 allocs/op
BenchmarkReadCompressedSQLiteFTS5Trigram-4     	      48	  21235303 ns/op	45970431 B/op	     148 allocs/op
BenchmarkReadCompressedHTTPSQLite-4            	  284820	      4341 ns/op	    3312 B/op	      15 allocs/op
```
