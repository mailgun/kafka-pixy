#!/bin/sh

# Make sure the script fails fast.
set -e
set -u

# Make a list of all non-vendored packages.
PKGS=$(go list ./... | grep -v "/tools\|/testhelpers\|/releases\|/cmd")
PKGS_DELIM=$(echo $PKGS | sed -e 's/ /,/g')

echo "Check that the code meets quality standards"
go fmt $PKGS
go vet $PKGS

echo "Run tests for all packages and aggregate coverage data to `cover.out`"
go test -v -p 1 --parallel=1 -timeout=600s -covermode=atomic -coverprofile=cover.out -coverpkg $PKGS_DELIM ./... -check.vv;

echo "Ship the coverage report to coveralls.io service"
go get github.com/mattn/goveralls
$HOME/gopath/bin/goveralls -coverprofile=cover.out -service=travis-ci
