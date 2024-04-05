package sqlitezstd_test

import (
	"database/sql"
	"fmt"
	"io"
	"math/rand"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"testing"

	sqlitezstd "github.com/jtarchie/sqlitezstd"
	_ "github.com/mattn/go-sqlite3" // ensure you import the SQLite3 driver
	"github.com/onsi/gomega/gexec"
	"github.com/pioz/faker"
)

//nolint: gochecknoglobals
var dbPath, zstPath string

// setupDB prepares a database for benchmarking.
// It returns the path of the created database and a cleanup function.
//nolint: cyclop
func setupDB(b *testing.B) (string, string) {
	b.Helper()

	if dbPath != "" {
		return dbPath, zstPath
	}

	_ = sqlitezstd.Init()

	buildPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	dbPath = filepath.Join(buildPath, "test.sqlite")

	client, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	_, err = client.Exec(`
		CREATE TABLE entries (
			value INTEGER,
			sentence TEXT
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	transaction, err := client.Begin()
	if err != nil {
		b.Fatalf("Failed to start transaction: %v", err)
	}

	defer func() { _ = transaction.Rollback() }()

	insert, err := transaction.Prepare("INSERT INTO entries (value, sentence) VALUES (?, ?)")
	if err != nil {
		b.Fatalf("Failed to insert prepare: %v", err)
	}
	defer insert.Close()

	for range 1_000_000 {
		//nolint: gosec
		_, err = insert.Exec(rand.Int63(), faker.Sentence())
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}

	_ = transaction.Commit()

	// index reduces number of page loads
	_, err = client.Exec(`
		CREATE INDEX aindex ON entries(value);
		CREATE VIRTUAL TABLE entries_fts USING fts5(sentence, tokenize="porter unicode61");
		INSERT INTO entries_fts(rowid, sentence)
		SELECT rowid, sentence FROM entries;
		INSERT INTO entries_fts(entries_fts) VALUES ('optimize');
		VACUUM;
	`)
	if err != nil {
		b.Fatalf("Failed to create index: %v", err)
	}

	// Assuming the compression step is the same as in the test,
	// and that it's already correctly set up and works.
	zstPath = dbPath + ".zst"

	command := exec.Command(
		"go", "run", "github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek",
		"-f", dbPath,
		"-o", zstPath,
		// "-q", "22",
	)

	session, err := gexec.Start(command, io.Discard, io.Discard)
	if err != nil {
		b.Fatalf("Failed to compress data: %v", err)
	}

	session.Wait("10s")

	return dbPath, zstPath
}

// Benchmark reading from the uncompressed SQLite file.
func BenchmarkReadUncompressedSQLite(b *testing.B) {
	dbPath, _ := setupDB(b)

	client, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	client.SetMaxOpenConns(max(4, runtime.NumCPU()))

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		var count int
		for pb.Next() {
			err = client.QueryRow("SELECT MAX(value) FROM entries").Scan(&count)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

func BenchmarkReadUncompressedSQLiteFTS5(b *testing.B) {
	dbPath, _ := setupDB(b)

	client, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	client.SetMaxOpenConns(max(4, runtime.NumCPU()))

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		var count int
		for pb.Next() {
			err = client.QueryRow("SELECT COUNT(*) FROM entries_fts WHERE entries_fts MATCH 'alligator'").Scan(&count)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

// Benchmark reading from the compressed SQLite file.
func BenchmarkReadCompressedSQLite(b *testing.B) {
	_, zstPath := setupDB(b)

	client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	client.SetMaxOpenConns(max(4, runtime.NumCPU()))

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		var count int
		for pb.Next() {
			err = client.QueryRow("SELECT MAX(value) FROM entries").Scan(&count)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

func BenchmarkReadCompressedSQLiteFTS5(b *testing.B) {
	_, zstPath := setupDB(b)

	client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	client.SetMaxOpenConns(max(4, runtime.NumCPU()))

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		var count int
		for pb.Next() {
			err = client.QueryRow("SELECT COUNT(*) FROM entries_fts WHERE entries_fts MATCH 'alligator'").Scan(&count)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}

func BenchmarkReadCompressedHTTPSQLite(b *testing.B) {
	_, zstPath := setupDB(b)

	zstDir := filepath.Dir(zstPath)

	server := httptest.NewServer(http.FileServer(http.Dir(zstDir)))
	defer server.Close()

	client, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s?vfs=zstd", server.URL, filepath.Base(zstPath)))
	if err != nil {
		b.Fatalf("Query failed: %v", err)
	}
	defer client.Close()

	client.SetMaxOpenConns(max(4, runtime.NumCPU()))

	b.ResetTimer() // Start timing now.

	b.RunParallel(func(pb *testing.PB) {
		var count int
		for pb.Next() {
			err = client.QueryRow("SELECT MAX(value) FROM entries").Scan(&count)
			if err != nil {
				b.Fatalf("Query failed: %v", err)
			}
		}
	})
}
