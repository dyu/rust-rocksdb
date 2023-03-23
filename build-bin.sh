#!/bin/sh

cargo build --release --bin rocksdb_backup \
--no-default-features \
--features snappy,zstd