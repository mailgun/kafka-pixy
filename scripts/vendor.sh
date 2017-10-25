#!/usr/bin/env bash

dep ensure
dep prune

# dep prune does poor job so we have to cleanup some unused source files.
find vendor/github.com/mailgun/holster/ -type f -maxdepth 1 -delete
find vendor/golang.org/x/text/internal/ -type f -maxdepth 1 -delete
find vendor/ -name *_test.go -delete
