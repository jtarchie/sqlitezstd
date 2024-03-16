package sqlitezstd_test

import (
	"database/sql"
	"fmt"
	"io"
	"os"
	"os/exec"
	"path/filepath"
	"testing"

	sqlitezstd "github.com/jtarchie/sqlitezstd"
	_ "github.com/mattn/go-sqlite3" // ensure you import the SQLite3 driver
	"github.com/onsi/gomega/gexec"
)

// setupDB prepares a database for benchmarking.
// It returns the path of the created database and a cleanup function.
func setupDB(b *testing.B) (string, string, func()) {
	b.Helper()

	_ = sqlitezstd.Init()

	buildPath, err := os.MkdirTemp("", "")
	if err != nil {
		b.Fatalf("Failed to create temp directory: %v", err)
	}

	dbPath := filepath.Join(buildPath, "test.sqlite")

	client, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}

	_, err = client.Exec(`
		CREATE TABLE entries (
			value INTEGER
		);
	`)
	if err != nil {
		b.Fatalf("Failed to create table: %v", err)
	}

	for id := 1; id <= 10_000; id++ {
		_, err = client.Exec("INSERT INTO entries (value) VALUES (?)", id)
		if err != nil {
			b.Fatalf("Failed to insert data: %v", err)
		}
	}

	// Assuming the compression step is the same as in the test,
	// and that it's already correctly set up and works.
	zstPath := dbPath + ".zst"

	command := exec.Command(
		"go", "run", "github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek",
		"-f", dbPath,
		"-o", zstPath,
		"-q", "22",
	)

	session, err := gexec.Start(command, io.Discard, io.Discard)
	if err != nil {
		b.Fatalf("Failed to compress data: %v", err)
	}

	session.Wait()

	// Cleanup function to remove temporary files.
	cleanup := func() {
		os.Remove(dbPath)
		os.Remove(zstPath)
		os.RemoveAll(buildPath)
	}

	return dbPath, zstPath, cleanup
}

// Benchmark reading from the uncompressed SQLite file.
func BenchmarkReadUncompressedSQLite(b *testing.B) {
	dbPath, _, cleanup := setupDB(b)
	defer cleanup()

	client, err := sql.Open("sqlite3", dbPath)
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	b.ResetTimer() // Start timing now.

	for i := 0; i < b.N; i++ {
		var count int

		err = client.QueryRow("SELECT MAX(value) FROM entries").Scan(&count)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}

// Benchmark reading from the compressed SQLite file.
func BenchmarkReadCompressedSQLite(b *testing.B) {
	_, zstPath, cleanup := setupDB(b)
	defer cleanup()

	client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
	if err != nil {
		b.Fatalf("Failed to open database: %v", err)
	}
	defer client.Close()

	b.ResetTimer() // Start timing now.

	for i := 0; i < b.N; i++ {
		var count int

		err = client.QueryRow("SELECT MAX(value) FROM entries").Scan(&count)
		if err != nil {
			b.Fatalf("Query failed: %v", err)
		}
	}
}
