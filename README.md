# SQLiteZSTD: Read-Only Access to Compressed SQLite Files

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
```

In this Go code example:

- The SQLiteZSTD library is initialized first with `sqlitezstd.Init()`.
- An SQL connection to a compressed SQLite database is established with
  `sql.Open()`.

The `sql.Open()` function takes as a parameter the path to the compressed SQLite
database, appended with a query string. Key query string parameters include:

- `vfs=zstd`: Ensures the ZSTD VFS is used.

## Performance

Here's a simple benchmark comparing performance between reading from an
uncompressed vs. a compressed SQLite database, involving the insertion of 10k
records and retrieval of the `MAX` value (without an index) and FTS5.

```
BenchmarkReadUncompressedSQLite-4       	  155996	      7491 ns/op	     473 B/op	      15 allocs/op
BenchmarkReadUncompressedSQLiteFTS5-4   	    2348	    474258 ns/op	     451 B/op	      15 allocs/op
BenchmarkReadCompressedSQLite-4         	  281256	      4072 ns/op	    2845 B/op	      15 allocs/op
BenchmarkReadCompressedSQLiteFTS5-4     	    2366	    488037 ns/op	   28936 B/op	      15 allocs/op
BenchmarkReadCompressedHTTPSQLite-4     	  275788	      4314 ns/op	    3399 B/op	      15 allocs/op
```
