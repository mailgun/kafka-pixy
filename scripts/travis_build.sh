#!/bin/sh

# Make sure the script fails fast.
set -e
set -u

# Make a list of all non-vendored packages.
PKGS=$(go list ./... | grep -v /vendor/ | grep -v /tools/ | grep -v /testhelpers/)
PKGS_DELIM=$(echo $PKGS | sed -e 's/ /,/g')

echo "Check that the code meets quality standards"
go fmt $PKGS
go vet $PKGS

echo "Run tests for all packages and aggregate coverage data to `cover.out`"
for P in $PKGS; do
    F=$(echo $P | sed -e 's/\//_/g').coverprofile
    go test -v -p 1 -timeout=120s -covermode=atomic -coverprofile=$F -coverpkg $PKGS_DELIM $P -check.vv;
done
go get github.com/wadey/gocovmerge
gocovmerge $(ls *.coverprofile) > cover.out

echo "Ship the coverage report to coveralls.io service"
go get github.com/mattn/goveralls
$HOME/gopath/bin/goveralls -coverprofile=cover.out -service=travis-ci
