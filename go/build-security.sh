#!/usr/bin/env bash
set -e

rm -rf tmp
mkdir -p tmp
cd tmp
git clone https://github.com/saichler/shared
cd ./shared/go/share/shallow_security
go build -buildmode=plugin -o security.so plugin.go ShallowSecurityProvider.go
mv security.so ../../../../../.
cd ../../../../../.
rm -rf tmp