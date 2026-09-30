module github.com/softwaredc/tachyon/bindings/go

go 1.21

// The C library is linked through cgo. If libtachyon is installed somewhere
// non-standard, point cgo at it:
//
//   CGO_CFLAGS="-I/path/to/include" \
//   CGO_LDFLAGS="-L/path/to/lib -ltachyon" \
//   go build ./...
//
// Runtime needs the shared library on the loader path:
//   LD_LIBRARY_PATH=/usr/local/lib ./yourprogram
