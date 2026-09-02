package sqlitezstd_test

import (
	"context"
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"testing"

	"github.com/georgysavva/scany/v2/sqlscan"
	_ "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
)

func TestSqliteZstd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SqliteZstd Suite")
}

const maxSize = 1_000_000

// trackingResponseWriter wraps http.ResponseWriter to track bytes written
type trackingResponseWriter struct {
	http.ResponseWriter
	bytesWritten int64
}

func (tw *trackingResponseWriter) Write(p []byte) (int, error) {
	n, err := tw.ResponseWriter.Write(p)
	tw.bytesWritten += int64(n)
	return n, err
}

func createDatabase() string {
	buildPath, err := os.MkdirTemp("", "")
	Expect(err).ToNot(HaveOccurred())

	dbPath := filepath.Join(buildPath, "test.sqlite")

	client, err := sql.Open("sqlite3", dbPath)
	Expect(err).ToNot(HaveOccurred())

	_, err = client.Exec(`
		CREATE TABLE entries (
			id INTEGER PRIMARY KEY
		);
	`)
	Expect(err).ToNot(HaveOccurred())

	tx, err := client.Begin()
	Expect(err).ToNot(HaveOccurred())
	defer func() { _ = tx.Rollback() }()

	stmt, err := tx.Prepare("INSERT INTO entries (id) VALUES (?)")
	Expect(err).ToNot(HaveOccurred())
	defer stmt.Close() //nolint: errcheck

	for id := 1; id <= maxSize; id++ {
		_, err = stmt.Exec(id)
		Expect(err).ToNot(HaveOccurred())
	}

	err = tx.Commit()
	Expect(err).ToNot(HaveOccurred())

	zstPath := dbPath + ".zst"

	Expect(compressSeekable(dbPath, zstPath, 0)).To(Succeed())

	return zstPath
}

func createComplexDatabase() (string, string) {
	buildPath, err := os.MkdirTemp("", "")
	Expect(err).ToNot(HaveOccurred())

	dbPath := filepath.Join(buildPath, "complex.sqlite")

	client, err := sql.Open("sqlite3", dbPath)
	Expect(err).ToNot(HaveOccurred())
	defer client.Close() //nolint: errcheck

	_, err = client.Exec(`
		CREATE TABLE users (
			id INTEGER PRIMARY KEY,
			name TEXT,
			age INTEGER
		);
		CREATE TABLE orders (
			id INTEGER PRIMARY KEY,
			user_id INTEGER,
			product TEXT,
			quantity INTEGER,
			FOREIGN KEY (user_id) REFERENCES users(id)
		);
	`)
	Expect(err).ToNot(HaveOccurred())

	tx, err := client.Begin()
	Expect(err).ToNot(HaveOccurred())
	defer func() { _ = tx.Rollback() }()

	userStmt, err := tx.Prepare("INSERT INTO users (name, age) VALUES (?, ?)")
	Expect(err).ToNot(HaveOccurred())
	defer userStmt.Close() //nolint: errcheck

	orderStmt, err := tx.Prepare("INSERT INTO orders (user_id, product, quantity) VALUES (?, ?, ?)")
	Expect(err).ToNot(HaveOccurred())
	defer orderStmt.Close() //nolint: errcheck

	for i := 1; i <= maxSize; i++ {
		_, err = userStmt.Exec(fmt.Sprintf("User%d", i), 20+(i%60))
		Expect(err).ToNot(HaveOccurred())

		_, err = orderStmt.Exec(i, fmt.Sprintf("Product%d", i%100), i%10+1)
		Expect(err).ToNot(HaveOccurred())
	}

	err = tx.Commit()
	Expect(err).ToNot(HaveOccurred())

	err = client.Close()
	Expect(err).ToNot(HaveOccurred())

	zstPath := dbPath + ".zst"

	Expect(compressSeekable(dbPath, zstPath, 16*1024)).To(Succeed())

	return dbPath, zstPath
}

var _ = Describe("SqliteZSTD", func() {
	It("can read from a compressed sqlite db", func() {
		zstPath := createDatabase()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close() //nolint: errcheck

		row := client.QueryRow("SELECT COUNT(*) FROM entries;")
		Expect(row.Err()).ToNot(HaveOccurred())

		var count int64
		err = row.Scan(&count)
		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(BeEquivalentTo(maxSize))
	})

	It("can handle multiple readers", func() {
		zstPath := createDatabase()

		waiter := &sync.WaitGroup{}

		for range 5 {
			waiter.Add(1)

			go func() {
				defer waiter.Done()
				defer GinkgoRecover()

				client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
				Expect(err).ToNot(HaveOccurred())
				defer client.Close() //nolint: errcheck

				for range 1_000 {
					row := client.QueryRow("SELECT * FROM entries ORDER BY RANDOM() LIMIT 1;")
					Expect(row.Err()).ToNot(HaveOccurred())
				}
			}()
		}

		waiter.Wait()
	})

	When("file does not exist", func() {
		It("returns an error", func() {
			client, err := sql.Open("sqlite3", "file:some.db?vfs=zstd")
			Expect(err).ToNot(HaveOccurred())
			defer client.Close() //nolint: errcheck

			row := client.QueryRow("SELECT * FROM entries ORDER BY RANDOM() LIMIT 1;")
			Expect(row.Err()).To(HaveOccurred())
		})
	})

	It("allows reading from HTTP server", func() {
		zstPath := createDatabase()
		zstDir := filepath.Dir(zstPath)
		server := httptest.NewServer(http.FileServer(http.Dir(zstDir)))
		defer server.Close()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s?vfs=zstd", server.URL, filepath.Base(zstPath)))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close() //nolint: errcheck

		row := client.QueryRow("SELECT COUNT(*) FROM entries;")
		Expect(row.Err()).ToNot(HaveOccurred())

		var count int64
		err = row.Scan(&count)
		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(BeEquivalentTo(maxSize))
	})

	It("ensures data integrity between compressed and uncompressed databases", func() {
		uncompressedPath, compressedPath := createComplexDatabase()

		uncompressedDB, err := sql.Open("sqlite3", uncompressedPath)
		Expect(err).ToNot(HaveOccurred())
		defer uncompressedDB.Close() //nolint: errcheck

		compressedDB, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", compressedPath))
		Expect(err).ToNot(HaveOccurred())
		defer compressedDB.Close() //nolint: errcheck

		row := compressedDB.QueryRow(`SELECT COUNT(*) FROM users;`)
		Expect(row.Err()).ToNot(HaveOccurred())

		var count int64
		Expect(row.Scan(&count)).ToNot(HaveOccurred())
		Expect(count).To(BeEquivalentTo(maxSize))

		query := `
			-- No longer required -- temp files go to the VFS underneath this
			-- one -- but kept so both databases sort the same way.
			PRAGMA temp_store = memory;
			SELECT u.age, COUNT(*) as order_count, SUM(o.quantity) as total_quantity
			FROM users u
			JOIN orders o ON u.id = o.user_id
			GROUP BY u.age
			ORDER BY u.age
		`

		type Result struct {
			Age           int
			OrderCount    int64
			TotalQuantity int64
		}

		var uncompressedResults, compressedResults []Result

		err = sqlscan.Select(context.Background(), uncompressedDB, &uncompressedResults, query)
		Expect(err).ToNot(HaveOccurred())

		err = sqlscan.Select(context.Background(), compressedDB, &compressedResults, query)
		Expect(err).ToNot(HaveOccurred())

		Expect(len(compressedResults)).To(BeNumerically(">", 0))
		Expect(len(compressedResults)).To(Equal(len(uncompressedResults)), "Compressed and uncompressed databases have different number of rows")

		for i := range uncompressedResults {
			Expect(compressedResults[i]).To(Equal(uncompressedResults[i]), "Row %d does not match between compressed and uncompressed databases", i)
		}
	})

	It("runs a query that spills to a temp file", func() {
		zstPath := createDatabase()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close() //nolint: errcheck

		// One connection, so the PRAGMAs apply to the one the query runs on.
		client.SetMaxOpenConns(1)

		// temp_store = FILE is the case that used to be impossible: the sorter
		// asks this VFS for a temp file, which it cannot create, so the open
		// went to the VFS underneath instead. A tiny page cache is what makes
		// the sort spill rather than stay in memory.
		_, err = client.Exec(`PRAGMA temp_store = FILE;`)
		Expect(err).ToNot(HaveOccurred())

		_, err = client.Exec(`PRAGMA cache_size = -16;`)
		Expect(err).ToNot(HaveOccurred())

		// Grouping and ordering by an expression rather than the primary key,
		// so sqlite has to sort every row instead of walking an index. No
		// LIMIT: a bounded sort is a top-N that never reaches a temp file.
		row := client.QueryRow(`
			SELECT COUNT(*) FROM (
				SELECT (id * 7) % 1000003 AS bucket, COUNT(*) AS n
				FROM entries GROUP BY bucket ORDER BY n, bucket
			)`)
		Expect(row.Err()).ToNot(HaveOccurred())

		var groups int64
		Expect(row.Scan(&groups)).ToNot(HaveOccurred())
		Expect(groups).To(BeNumerically(">", 0))
	})

	It("survives a failed open of another database", func() {
		_, zstPath := createComplexDatabase()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close() //nolint: errcheck

		// One connection with a tiny page cache, so the query after the failed
		// open has to fault pages in through the VFS rather than answer from
		// cache -- which is exactly the read that used to break.
		client.SetMaxOpenConns(1)
		_, err = client.Exec(`PRAGMA cache_size = -16;`)
		Expect(err).ToNot(HaveOccurred())

		var users int64
		Expect(client.QueryRow(`SELECT COUNT(*) FROM users`).Scan(&users)).To(Succeed())
		Expect(users).To(BeEquivalentTo(maxSize))

		// A failed open used to close the FIRST file opened through the VFS --
		// the healthy connection above -- because sqlite calls xClose on a
		// failed open and the unregistered file's zero id aliased it. The
		// connection then failed every uncached read with "SQL logic error".
		missing, err := sql.Open("sqlite3", fmt.Sprintf("%s.does-not-exist?vfs=zstd", zstPath))
		Expect(err).ToNot(HaveOccurred())
		Expect(missing.Ping()).ToNot(Succeed())
		Expect(missing.Close()).To(Succeed())

		// orders has not been touched, so counting it needs reads the cache
		// cannot answer.
		var orders int64
		Expect(client.QueryRow(`SELECT COUNT(*) FROM orders`).Scan(&orders)).To(Succeed())
		Expect(orders).To(BeEquivalentTo(maxSize))
	})

	It("uses HTTP Range headers and only downloads needed bytes", func() {
		zstPath := createDatabase()
		zstDir := filepath.Dir(zstPath)

		// Track HTTP requests
		var totalBytesServed int64
		var rangeRequestCount int64
		var mu sync.Mutex

		// Get the actual file size
		fileInfo, err := os.Stat(zstPath)
		Expect(err).ToNot(HaveOccurred())
		fileSize := fileInfo.Size()

		// Create a custom handler that tracks requests
		handler := http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
			// Check if Range header is present
			rangeHeader := r.Header.Get("Range")
			if rangeHeader != "" {
				mu.Lock()
				rangeRequestCount++
				mu.Unlock()
			}

			// Open the file
			file, err := os.Open(filepath.Join(zstDir, filepath.Base(zstPath))) //nolint: gosec
			if err != nil {
				http.Error(w, err.Error(), http.StatusInternalServerError)
				return
			}
			defer file.Close() //nolint: errcheck

			// Wrap the response writer to track bytes
			tw := &trackingResponseWriter{
				ResponseWriter: w,
			}

			// Use http.ServeContent which properly handles Range requests
			http.ServeContent(tw, r, filepath.Base(zstPath), fileInfo.ModTime(), file)

			// Track total bytes served
			mu.Lock()
			totalBytesServed += tw.bytesWritten
			mu.Unlock()
		})

		server := httptest.NewServer(handler)
		defer server.Close()

		// Open database and perform a simple query
		client, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s?vfs=zstd", server.URL, filepath.Base(zstPath)))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close() //nolint: errcheck

		// Perform a simple query that should only require reading a small portion
		// of the database (reading a single row by primary key)
		row := client.QueryRow("SELECT id FROM entries WHERE id = 1;")
		Expect(row.Err()).ToNot(HaveOccurred())

		var id int64
		err = row.Scan(&id)
		Expect(err).ToNot(HaveOccurred())
		Expect(id).To(BeEquivalentTo(1))

		mu.Lock()
		finalBytesServed := totalBytesServed
		finalRangeCount := rangeRequestCount
		mu.Unlock()

		// Verify Range headers were used
		Expect(finalRangeCount).To(BeNumerically(">", 0), "Expected Range requests to be made")

		// The key assertion: we should NOT download the entire file for a simple single-row query.
		// A single-row primary-key lookup against a 1M-row single-column table should touch only a
		// handful of pages/frames, so anything approaching even a few percent indicates a regression
		// (e.g. a broken cache re-fetching whole frames, or a fallback to large reads).
		percentDownloaded := float64(finalBytesServed) / float64(fileSize) * 100
		Expect(percentDownloaded).To(BeNumerically("<", 5.0),
			"Should download less than 5%% of file for single-row query, but downloaded %.2f%%", percentDownloaded)
	})
})
