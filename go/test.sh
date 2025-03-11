#!/usr/bin/env bash

# Fail on errors and don't open cover file
set -e
# clean up
rm -rf go.sum
rm -rf go.mod
rm -rf vendor

# fetch dependencies
go mod init
GOPROXY=direct GOPRIVATE=github.com go mod tidy

rm -rf tmp
mkdir -p tmp
cd tmp
git clone https://github.com/saichler/shared
cd ./shared/go/share/shallow_security
go build -buildmode=plugin -o security.so plugin.go ShallowSecurityProvider.go
mv security.so ../../../../../.
cd ../../../../../.
rm -rf tmp

go mod vendor

# Run unit tests with coverage
go test -tags=unit -v -coverpkg=./points/... -coverprofile=cover.html ./... --failfast

# Open the coverage report in a browser
go tool cover -html=cover.html
