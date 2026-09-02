module github.com/jtarchie/sqlitezstd

go 1.25.0

require (
	github.com/SaveTheRbtz/zstd-seekable-format-go/pkg v0.10.0
	github.com/brianvoe/gofakeit/v7 v7.15.0
	github.com/georgysavva/scany/v2 v2.1.4
	github.com/klauspost/compress v1.19.1
	github.com/mattn/go-sqlite3 v1.14.49
	github.com/onsi/ginkgo/v2 v2.32.0
	github.com/onsi/gomega v1.42.1
	github.com/psanford/httpreadat v0.1.0
	github.com/psanford/sqlite3vfs v0.0.0-20260519004904-f9180fa2acc9
)

require (
	github.com/Masterminds/semver/v3 v3.5.0 // indirect
	github.com/cespare/xxhash/v2 v2.3.0 // indirect
	github.com/go-logr/logr v1.4.4 // indirect
	github.com/go-task/slim-sprig/v3 v3.0.0 // indirect
	github.com/google/go-cmp v0.7.0 // indirect
	github.com/google/pprof v0.0.0-20260802141513-ef3492d7dac3 // indirect
	go.yaml.in/yaml/v3 v3.0.5 // indirect
	golang.org/x/mod v0.38.0 // indirect
	golang.org/x/net v0.57.0 // indirect
	golang.org/x/sync v0.22.0 // indirect
	golang.org/x/sys v0.47.0 // indirect
	golang.org/x/text v0.40.0 // indirect
	golang.org/x/tools v0.48.0 // indirect
)

// Temporary, until psanford/sqlite3vfs#20 and #21 land. #20 adds VFSFind,
// which is what lets Open hand temp files to the VFS underneath this one.
// #21 stops a failed open from closing an unrelated live file (observed in
// production as hours of "SQL logic error" / fts5 corruption reports against
// an intact database). Drop this and bump the require above once both merge.
replace github.com/psanford/sqlite3vfs => github.com/jtarchie/sqlite3vfs v0.0.0-20260902025759-f3d396eb1602
