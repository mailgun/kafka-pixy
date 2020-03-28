#!/bin/sh

# Make sure the script fails fast.
set -e
set -u

echo "Check that the code meets quality standards"
go fmt ./...
go vet ./...

echo "Run tests for all packages and aggregate coverage data to `cover.out`"
PKGS=$(go list ./... | grep -v "/testhelpers\|/releases\|/cmd")
go test -v -p 1 --parallel=1 -timeout=600s -covermode=atomic -coverprofile=cover.out $PKGS -check.vv;

echo "Ship the coverage report to coveralls.io service"
go get github.com/mattn/goveralls
$HOME/gopath/bin/goveralls -coverprofile=cover.out -service=travis-ci
