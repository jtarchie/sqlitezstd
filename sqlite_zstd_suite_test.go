package sqlitezstd_test

import (
	"database/sql"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"os/exec"
	"path/filepath"
	"sync"
	"testing"

	sqlitezstd "github.com/jtarchie/sqlitezstd"
	. "github.com/onsi/ginkgo/v2"
	. "github.com/onsi/gomega"
	"github.com/onsi/gomega/gexec"
)

func TestSqliteZstd(t *testing.T) {
	RegisterFailHandler(Fail)
	RunSpecs(t, "SqliteZstd Suite")
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

	for id := 1; id <= 1000; id++ {
		_, err = client.Exec("INSERT INTO entries (id) VALUES (?)", id)
		Expect(err).ToNot(HaveOccurred())
	}

	zstPath := dbPath + ".zst"

	command := exec.Command(
		"go", "run", "github.com/SaveTheRbtz/zstd-seekable-format-go/cmd/zstdseek",
		"-f", dbPath,
		"-o", zstPath,
	)

	session, err := gexec.Start(command, GinkgoWriter, GinkgoWriter)
	Expect(err).ToNot(HaveOccurred())
	Eventually(session).Should(gexec.Exit(0))

	return zstPath
}

var _ = Describe("SqliteZSTD", func() {
	BeforeEach(func() {
		err := sqlitezstd.Init()
		Expect(err).ToNot(HaveOccurred())
	})

	It("can read from a compressed sqlite db", func() {
		zstPath := createDatabase()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close()

		row := client.QueryRow("SELECT COUNT(*) FROM entries;")
		Expect(row.Err()).ToNot(HaveOccurred())

		var count int64
		err = row.Scan(&count)
		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(BeEquivalentTo(1000))
	})

	It("can handle multiple readers", func() {
		zstPath := createDatabase()

		waiter := &sync.WaitGroup{}

		for i := 0; i < 5; i++ {
			waiter.Add(1)

			go func() {
				defer waiter.Done()
				defer GinkgoRecover()

				client, err := sql.Open("sqlite3", fmt.Sprintf("%s?vfs=zstd", zstPath))
				Expect(err).ToNot(HaveOccurred())
				defer client.Close()

				for i := 0; i < 1_000; i++ {
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
			defer client.Close()

			row := client.QueryRow("SELECT * FROM entries ORDER BY RANDOM() LIMIT 1;")
			Expect(row.Err()).To(HaveOccurred())
		})
	})

	It("does something", func() {
		zstPath := createDatabase()
		zstDir := filepath.Dir(zstPath)
		server := httptest.NewServer(http.FileServer(http.Dir(zstDir)))
		defer server.Close()

		client, err := sql.Open("sqlite3", fmt.Sprintf("%s/%s?vfs=zstd", server.URL, filepath.Base(zstPath)))
		Expect(err).ToNot(HaveOccurred())
		defer client.Close()

		row := client.QueryRow("SELECT COUNT(*) FROM entries;")
		Expect(row.Err()).ToNot(HaveOccurred())

		var count int64
		err = row.Scan(&count)
		Expect(err).ToNot(HaveOccurred())
		Expect(count).To(BeEquivalentTo(1000))
	})
})
