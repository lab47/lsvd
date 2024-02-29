#!/bin/sh

set -x

rm -rf ~/tmp/data/lsvdinttest/*
rm data/cache/*
rm tmp/blah/*
go build -o bin/lsvd ./cmd/lsvd
./bin/lsvd volume init -c ./hack/minio.hcl -n ubuntu -s 50G
# ./bin/lsvd dd -c ./hack/minio.hcl -n ubuntu -p ./tmp/blah -i https://cloud-images.ubuntu.com/noble/current/noble-server-cloudimg-arm64.img
./bin/lsvd dd -c ./hack/minio.hcl -n ubuntu -p ./tmp/blah -i noble-server-cloudimg-arm64.img --expand

echo "should be: 9ac187b2b662e544637f0f22530120b5ee4d11abda300000a539a6ebbcf8500d"
#echo "should be: 749eb9015e1cb492639a9a0b9e844bb1c42ffddb84bb6bc0c3af9f0c716a0906"
