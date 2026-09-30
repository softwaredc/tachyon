module github.com/softwaredc/tachyon/examples/go

go 1.21

// The binding module lives in this repository, so replace it with the local
// path. Change softwaredc to your GitHub handle once you fork.
require github.com/softwaredc/tachyon/bindings/go v0.0.0

replace github.com/softwaredc/tachyon/bindings/go => ../../bindings/go
